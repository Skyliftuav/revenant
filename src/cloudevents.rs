// src/cloudevents.rs

use base64::Engine;
// Add the necessary use statements
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandPayload {
    pub command: String,
    pub parameters: HashMap<String, String>,
    pub drone_id: String,
    pub flight_id: u32,
    pub operation_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitDronePayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drone_id: Option<String>,
    pub flight_id: u32,
    pub operation_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flight_profile: Option<FlightProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightProfile {
    pub max_horizontal_speed: f32,
    pub max_vertical_speed: f32,
    pub max_yaw_rate: f32,
    pub responsiveness: f32,
}

// CloudEvent wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudEvent {
    // Required Attributes:
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub specversion: String,

    // Optional Attributes:
    pub datacontenttype: Option<String>,
    pub dataschema: Option<String>,
    pub subject: Option<String>,
    pub time: Option<String>,

    // Data:
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<CloudEventData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,

    // Extension Attributes:
    #[serde(flatten)]
    pub extensions: HashMap<String, serde_json::Value>,
}

impl ToString for CloudEvent {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CloudEventData {
    Command(CommandPayload),
    InitDrone(InitDronePayload),
    EventData(Value),
}

impl CloudEvent {
    pub fn new(source: String, event_type: String, data: Option<CloudEventData>) -> Self {
        CloudEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source,
            event_type,
            specversion: "1.0".to_string(),
            datacontenttype: Some("application/json".to_string()),
            dataschema: None,
            subject: None,
            time: None,
            data,
            data_base64: None,
            extensions: HashMap::new(),
        }
    }

    pub fn add_extension(&mut self, key: String, value: serde_json::Value) -> &mut Self {
        self.extensions.insert(key, value);
        self
    }

    pub fn set_subject(&mut self, subject: String) -> &mut Self {
        self.subject = Some(subject);
        self
    }

    pub fn set_dataschema(&mut self, dataschema: String) -> &mut Self {
        self.dataschema = Some(dataschema);
        self
    }

    pub fn set_datacontenttype(&mut self, datacontenttype: String) -> &mut Self {
        self.datacontenttype = Some(datacontenttype);
        self
    }

    pub fn encode_data_base64(&mut self, data: Vec<u8>) -> &mut Self {
        // Encode the data to base64 and set the data_base64 field
        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
        self.data_base64 = Some(encoded);
        self
    }

    pub fn build(&mut self) -> Self {
        self.time = Some(chrono::Utc::now().to_rfc3339());
        self.clone()
    }
}
