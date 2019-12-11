use crate::{
    buffers::Acker,
    event::{self, Event},
    sinks::util::MetadataFuture,
    topology::config::{DataType, SinkConfig, SinkContext, SinkDescription},
};
use futures::{
    future::{self, poll_fn, IntoFuture},
    stream::FuturesUnordered,
    Async, AsyncSink, Future, Poll, Sink, StartSend, Stream,
};
use lapin_futures::{
    auth::SASLMechanism,
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Client, ConfirmationFuture, ConnectionProperties, ExchangeKind,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SASLMechanismDef {
    AMQPlain,
    External,
    Plain,
    RabbitCrDemo,
}

impl SASLMechanismDef {
    pub fn to_sasl_mechanism(&self) -> SASLMechanism {
        match &self {
            SASLMechanismDef::AMQPlain => SASLMechanism::AMQPlain,
            SASLMechanismDef::External => SASLMechanism::External,
            SASLMechanismDef::Plain => SASLMechanism::Plain,
            SASLMechanismDef::RabbitCrDemo => SASLMechanism::RabbitCrDemo,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConnectionPropertiesDef {
    pub mechanism: SASLMechanismDef,
    pub locale: String,
    pub client_properties: FieldTable,
    pub max_executor_threads: usize,
}

impl Default for ConnectionPropertiesDef {
    fn default() -> ConnectionPropertiesDef {
        ConnectionPropertiesDef {
            mechanism: SASLMechanismDef::Plain,
            locale: "en_US".into(),
            client_properties: FieldTable::default(),
            max_executor_threads: 1,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ExchangeDeclareKindDef {
    Direct,
    Fanout,
    Headers,
    Topic,
}

fn default_exchange_declare_kind() -> ExchangeDeclareKindDef {
    ExchangeDeclareKindDef::Direct
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExchangeDeclareOptionsDef {
    #[serde(default = "default_exchange_declare_kind")]
    pub kind: ExchangeDeclareKindDef,
    pub passive: Option<bool>,
    pub durable: Option<bool>,
    pub auto_delete: Option<bool>,
    pub internal: Option<bool>,
    pub nowait: Option<bool>,
    pub field_table: FieldTable,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct BasicPublishOptionsDef {
    pub mandatory: bool,
    pub immediate: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RabbitMQSinkConfig {
    addr: String,
    basic_publish_options: BasicPublishOptionsDef,
    connection_properties: ConnectionPropertiesDef,
    encoding: Encoding,
    exchange: String,
    routing_key: String,
    exchange_declare_options: Option<ExchangeDeclareOptionsDef>,
}

impl RabbitMQSinkConfig {
    pub fn connection_properties(&self) -> ConnectionProperties {
        ConnectionProperties {
            mechanism: self.connection_properties.mechanism.to_sasl_mechanism(),
            locale: self.connection_properties.locale.clone(),
            client_properties: self.connection_properties.client_properties.clone(),
            executor: None,
            max_executor_threads: self.connection_properties.max_executor_threads,
        }
    }

    pub fn exchange_declare_options(
        &self,
    ) -> Option<(ExchangeKind, ExchangeDeclareOptions, FieldTable)> {
        if let Some(opts) = &self.exchange_declare_options {
            let kind = match &opts.kind {
                ExchangeDeclareKindDef::Direct => ExchangeKind::Direct,
                ExchangeDeclareKindDef::Fanout => ExchangeKind::Fanout,
                ExchangeDeclareKindDef::Headers => ExchangeKind::Headers,
                ExchangeDeclareKindDef::Topic => ExchangeKind::Topic,
            };
            Some((
                kind,
                ExchangeDeclareOptions {
                    passive: opts.passive.unwrap_or(false),
                    durable: opts.durable.unwrap_or(true),
                    auto_delete: opts.auto_delete.unwrap_or(false),
                    internal: opts.internal.unwrap_or(false),
                    nowait: opts.nowait.unwrap_or(false),
                },
                opts.field_table.clone(),
            ))
        } else {
            None
        }
    }

    pub fn basic_publish_options(&self) -> BasicPublishOptions {
        BasicPublishOptions {
            immediate: self.basic_publish_options.immediate,
            mandatory: self.basic_publish_options.mandatory,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Text,
    Json,
}

pub struct RabbitMQSink {
    acker: Acker,
    basic_publish_options: BasicPublishOptions,
    channel: lapin_futures::Channel,
    encoding: Encoding,
    exchange: String,
    in_flight: FuturesUnordered<MetadataFuture<ConfirmationFuture<()>, usize>>,
    seqno: usize,
    routing_key: String,
    pending_acks: HashSet<usize>,
}

impl RabbitMQSink {
    fn new(config: RabbitMQSinkConfig, acker: Acker) -> crate::Result<Self> {
        let channel = Client::connect(&config.addr, config.connection_properties())
            .and_then(|client| client.create_channel())
            .wait()?;
        if let Some((kind, opts, field_table)) = config.exchange_declare_options() {
            channel
                .exchange_declare(&config.exchange, kind, opts, field_table)
                .wait()?;
        }
        Ok(RabbitMQSink {
            acker,
            basic_publish_options: config.basic_publish_options(),
            channel,
            encoding: config.encoding,
            exchange: config.exchange,
            in_flight: FuturesUnordered::new(),
            seqno: 0,
            routing_key: config.routing_key,
            pending_acks: HashSet::new(),
        })
    }
}

inventory::submit! {
    SinkDescription::new_without_default::<RabbitMQSinkConfig>("rabbitmq")
}

#[typetag::serde(name = "rabbitmq")]
impl SinkConfig for RabbitMQSinkConfig {
    fn build(&self, cx: SinkContext) -> crate::Result<(super::RouterSink, super::Healthcheck)> {
        let sink = RabbitMQSink::new(self.clone(), cx.acker())?;
        let hc = healthcheck(self.clone());
        Ok((Box::new(sink), hc))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }

    fn sink_type(&self) -> &'static str {
        "rabbitmq"
    }
}

impl Sink for RabbitMQSink {
    type SinkItem = Event;
    type SinkError = ();

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let payload = encode_event(&item, &self.encoding);
        let future = self.channel.basic_publish(
            &self.exchange,
            &self.routing_key,
            payload,
            self.basic_publish_options.clone(),
            BasicProperties::default(),
        );
        self.in_flight.push(future.join(future::ok(self.seqno)));
        self.pending_acks.insert(self.seqno);
        self.seqno += 1;

        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        loop {
            match self.in_flight.poll() {
                // nothing ready yet
                Ok(Async::NotReady) => return Ok(Async::NotReady),

                // nothing in flight
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),

                // request finished, check for success
                Ok(Async::Ready(Some(((), seqno)))) => {
                    if self.pending_acks.remove(&seqno) {
                        self.acker.ack(1);
                        trace!("published message to rabbitmq");
                    } else {
                        error!("message already published");
                    }
                }

                Err(e) => error!("publishing message failed: {}", e),
            }
        }
    }
}

fn healthcheck(config: RabbitMQSinkConfig) -> super::Healthcheck {
    let check = poll_fn(move || {
        tokio_threadpool::blocking(|| {
            Client::connect(&config.addr, config.connection_properties())
                .map(|_| ())
                .map_err(|err| err.into())
        })
    })
    .map_err(|err| err.into())
    .and_then(|result| result.into_future());

    Box::new(check)
}

fn encode_event(event: &Event, encoding: &Encoding) -> Vec<u8> {
    let payload = match encoding {
        &Encoding::Json => serde_json::to_vec(&event.as_log().clone().unflatten()).unwrap(),
        &Encoding::Text => event
            .as_log()
            .get(&event::MESSAGE)
            .map(|v| v.as_bytes().to_vec())
            .unwrap_or(Vec::new()),
    };

    payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn rabbitmq_encode_event_text() {
        let message = "hello world".to_string();
        let bytes = encode_event(&message.clone().into(), &Encoding::Text);
        assert_eq!(&bytes[..], message.as_bytes());
    }

    #[test]
    fn rabbitmq_encode_event_json() {
        let message = "hello world".to_string();
        let event = Event::from(message.clone());
        let bytes = encode_event(&event, &Encoding::Json);
        let map: HashMap<String, String> = serde_json::from_slice(&bytes[..]).unwrap();
        assert_eq!(map[&event::MESSAGE.to_string()], message);
    }
}

#[cfg(feature = "rabbitmq-integration-tests")]
#[cfg(test)]
mod integration_test {
    use super::*;
    use crate::test_util::{block_on, random_lines_with_stream, random_string};
    use lapin_futures::options::BasicConsumeOptions;
    use std::{collections::HashSet, iter::FromIterator};

    #[test]
    fn publish_messages() {
        let queue_name = format!("test-{}", random_string(10));
        let addr = String::from("amqp://127.0.0.1:5672/%2f");
        let config = RabbitMQSinkConfig {
            addr: addr.clone(),
            basic_publish_options: BasicPublishOptionsDef::default(),
            connection_properties: ConnectionPropertiesDef::default(),
            encoding: Encoding::Text,
            exchange: String::from(""),
            field_table: FieldTable::default(),
            queue_name: queue_name.clone(),
            queue_declare_options: ExchangeDeclareOptionsDef::default(),
        };
        // publish messages to test rabbit queue
        let (acker, ack_counter) = Acker::new_for_testing();
        let rabbit = RabbitMQSink::new(config, acker).unwrap();
        let number_of_events = 1000;
        let (input, events) = random_lines_with_stream(100, number_of_events);
        let pump = rabbit.send_all(events);
        block_on(pump).unwrap();
        let mut messages: HashSet<String> = HashSet::from_iter(input);

        // create consumer to check the existence of the previously pushed messages
        let channel = Client::connect(&addr, ConnectionProperties::default())
            .and_then(|client| client.create_channel())
            .wait()
            .unwrap();
        let consumer_name = format!("consumer-{}", random_string(5));
        let consumer = channel
            .queue_declare(
                &queue_name,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .and_then(|queue| {
                channel.basic_consume(
                    &queue,
                    &consumer_name,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
            })
            .wait()
            .unwrap();
        // check that all messages exist in rabbitmq
        let mut counter = 0;
        for item in consumer.wait() {
            match item {
                Ok(message) => {
                    let string_message = String::from_utf8_lossy(&message.data);
                    messages.remove(&string_message[..]);
                    channel.basic_ack(message.delivery_tag, false);
                }
                Err(e) => error!("failed to run rabbitmq test: {}", e),
            }
            counter += 1;
            if counter == number_of_events {
                break;
            }
        }
        assert_eq!(messages.len(), 0);
        assert_eq!(
            ack_counter.load(std::sync::atomic::Ordering::Relaxed),
            number_of_events
        );
    }
}
