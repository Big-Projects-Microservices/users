use bson::{Document, from_slice};
use futures_util::stream::StreamExt;
use lapin::{
    options::*, types::FieldTable, Connection, ConnectionProperties, ConsumerDelegate,
    message::Delivery,
};
use serde::{Deserialize, Serialize};
use std::{env, error::Error};
use tokio::task;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct UserMessage {
    username: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address =
        env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672/%2f".into());

    let conn = Connection::connect(&address, ConnectionProperties::default())
        .await?;

    let channel = conn.create_channel().await?;

    // Создаем очередь и биндим к exchange
    let queue = channel
        .queue_declare(
            "user_create_queue",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            queue.name().as_str(),
            "user_exchange",
            "user.new",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "user_create_queue",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("Waiting for messages...");

    while let Some(result) = consumer.next().await {
        match result {
            Ok(delivery) => {
                handle_message(delivery).await?;
            }
            Err(e) => {
                eprintln!("Error while consuming: {}", e);
            }
        }
    }

    Ok(())
}

async fn handle_message(delivery: Delivery) -> Result<(), Box<dyn Error>> {
    println!("Message handling...");
    {
        let data = &delivery.data;
        let doc: Document = from_slice(&data)?;
        let user: UserMessage = bson::from_document(doc)?;

        info!("Received user: {:?}", user);
    }

    // подтверждаем получение
    delivery.ack(BasicAckOptions::default()).await?;

    Ok(())
}

