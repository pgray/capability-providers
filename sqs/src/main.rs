//! Implementation for wasmcloud:messaging
//!
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info, instrument};
use wasmbus_rpc::{core::LinkDefinition, provider::prelude::*};
use wasmcloud_interface_messaging::{
    Messaging, MessagingReceiver, PubMessage, ReplyMessage, RequestMessage,
};

use serde::{Deserialize, Serialize};

use aws_config as aws;
use aws_sdk_sqs as sqs;
// use aws_types::os_shim_internal::Env;

const DEFAULT_ACTOR_NAME: &str = "pgray";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    provider_main(
        SqsProvider::default(),
        Some("SQS Messaging Provider".to_string()),
    )?;
    eprintln!("SQS messaging provider exiting");
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SQSConfig {
    #[serde(default)]
    aws_secret_access_key: Option<String>,
    #[serde(default)]
    aws_access_key_id: Option<String>,
    #[serde(default)]
    aws_region: Option<String>,
    #[serde(default)]
    queue_name: Option<String>,
    #[serde(default)]
    create_queue_if_missing: Option<bool>,
    #[serde(default)]
    message_auto_delete: Option<bool>,
}

//impl SQSConfig {
//    fn new_from(value: &HashMap<String, String>) -> RpcResult<SQSConfig> {
//        let mut config = SQSConfig::default();
//        Ok(config)
//    }
//}

/// SQS implementation for wasmcloud:messaging
#[derive(Default, Clone, Provider)]
#[services(Messaging)]
struct SqsProvider {
    actors: Arc<RwLock<HashMap<String, sqs::Client>>>,
}

//impl SqsProvider {
//    async fn create_client(aaki: String, asak: String) -> Result<sqs::Client, RpcError> {
//        let env = Env::from_slice(&[("AWS_ACCESS_KEY_ID", aaki), ("AWS_SECRET_ACCESS_KEY", asak)]);
//        let loader = from_env().configure(ProviderConfig::empty().with_env(env));
//        let cli = sqs::Client
//        Ok()
//    }
//}
// use default implementations of provider message handlers
impl ProviderDispatch for SqsProvider {}

/// Handle provider control commands
/// put_link (new actor link command), del_link (remove link command), and shutdown
#[async_trait]
impl ProviderHandler for SqsProvider {
    /// Provider should perform any operations needed for a new link,
    /// including setting up per-actor resources, and checking authorization.
    /// If the link is allowed, return true, otherwise return false to deny the link.
    #[instrument(level = "info", skip(self))]
    async fn put_link(&self, ld: &LinkDefinition) -> RpcResult<bool> {
        // right now we need our wasmcloud host to have env vars set on its shell/container
        let config = aws::from_env().load().await;
        let client = sqs::Client::new(&config);

        debug!("putting link for actor {:?}", ld);
        let mut update_map = self.actors.write().unwrap();
        update_map.insert(DEFAULT_ACTOR_NAME.to_string(), client);
        Ok(true)
    }

    /// Handle notification that a link is dropped: close the connection
    #[instrument(level = "info", skip(self))]
    async fn delete_link(&self, actor_id: &str) {
        debug!("deleting link for actor {}", actor_id);
        let actor = DEFAULT_ACTOR_NAME.to_string();
        let mut aw = self.actors.write().unwrap();
        if aw.remove(&actor).is_some() {
            info!("sqs closing connection for actor {}", actor)
        }
    }

    /// Handle shutdown request with any cleanup necessary
    async fn shutdown(&self) -> std::result::Result<(), Infallible> {
        let mut aw = self.actors.write().unwrap();
        aw.clear();
        Ok(())
    }
}

/// Handle Messaging methods
#[async_trait]
impl Messaging for SqsProvider {
    #[instrument(level = "debug", skip(self, msg), fields(subject = %msg.subject, reply_to = ?msg.reply_to, body_len = %msg.body.len()))]
    async fn publish(&self, _ctx: &Context, msg: &PubMessage) -> RpcResult<()> {
        debug!("Publishing message: {:?}", msg);
        let actor = DEFAULT_ACTOR_NAME.to_string();
        let cli = { self.actors.read().unwrap().get(&actor).unwrap().clone() };
        let qurls = cli.list_queues().send().await.unwrap().clone();
        let qurl = qurls.queue_urls().unwrap().first().unwrap();

        match cli
            .send_message()
            .message_body("ok".to_string())
            .queue_url(qurl.clone())
            .send()
            .await
        {
            Ok(resp) => debug!("{:?}", resp),
            Err(e) => error!("{}", e),
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, msg), fields(subject = %msg.subject))]
    async fn request(&self, _ctx: &Context, msg: &RequestMessage) -> RpcResult<ReplyMessage> {
        debug!("Sending message request: {:?}", msg);
        let actor = DEFAULT_ACTOR_NAME.to_string();
        let cli = { self.actors.read().unwrap().get(&actor).unwrap().clone() };
        let qurls = cli.list_queues().send().await.unwrap().clone();
        let qurl = qurls.queue_urls().unwrap().first().unwrap();
        let msg = cli
            .receive_message()
            .queue_url(qurl.clone())
            .send()
            .await
            .unwrap();

        Ok(ReplyMessage {
            subject: "hello".to_string(),
            body: msg
                .messages()
                .unwrap()
                .first()
                .unwrap()
                .body()
                .unwrap()
                .as_bytes()
                .to_owned(),
            reply_to: None,
        })
    }
}
