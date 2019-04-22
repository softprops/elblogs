use chrono::prelude::{DateTime, Utc};
use flate2::read::GzDecoder;
use futures::future::{self, Future};
use recap::Recap;
use rusoto_core::Region;
use rusoto_s3::{GetObjectError, GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::io::Error as IoError;
use structopt::StructOpt;
use tokio::io::read_to_end;

#[derive(StructOpt)]
struct ElbLogs {
    /// full bucket path including bucket until region
    bucket_path: String,
}

#[derive(Debug)]
enum Error {
    Get(GetObjectError),
    Io(IoError),
}

impl From<GetObjectError> for Error {
    fn from(err: GetObjectError) -> Self {
        Error::Get(err)
    }
}

impl From<IoError> for Error {
    fn from(err: IoError) -> Self {
        Error::Io(err)
    }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Type {
    Http,
    Https,
    H2,
    Ws,
    Wss,
}

impl Default for Type {
    fn default() -> Self {
        Type::Http
    }
}

// https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format
#[derive(Default, Debug, Deserialize, Recap)]
#[recap(regex = r#"(?x)
    (?P<request_type>http|https|h2|ws|wss)
    \s
    (?P<timestamp>[\S]+)
    \s
    (?P<elb>[\S]+)
    \s
    (?P<client>[\S]+)
    \s
    (?P<target>[\S]+)
    \s
    (?P<request_processing_time>[\S]+)
    \s
    (?P<target_processing_time>[\S]+)
    \s
    (?P<response_processing_time>[\S]+)
    \s
    (?P<elb_status_code>[\S]+)
    \s
    (?P<target_status_code>[\S]+)
    \s
    (?P<received_bytes>[\S]+)
    \s
    (?P<sent_bytes>[\S]+)
    \s
    "(?P<request>[^"]+)"
    \s
    "(?P<user_agent>[^"]*)"
    \s
    (?P<ssl_cipher>[\S]+)
    \s
    (?P<ssl_protocol>[\S]+)
    \s
    (?P<target_group_arn>[\S]+)
    \s
    "(?P<trace_id>[^"]+)"
    \s
    "(?P<domain_name>[^"]+)"
    \s
    "(?P<chosen_cert_arn>[^"]+)"
    \s
    (?P<matched_rule_priority>[\S]+)
    \s
    (?P<request_creation_time>[\S]+)
    \s
    "(?P<actions_executed>[^"]+)"
    \s
    "(?P<redirect_url>[^"]+)"
    \s
    "(?P<error_reason>[^"]+)"
"#)]
struct Request {
    request_type: String,
    timestamp: String,
    elb: String,
    client: String,
    target: String,
    request_processing_time: f32,
    target_processing_time: f32,
    response_processing_time: f32,
    elb_status_code: u16,
    target_status_code: u16,
    received_bytes: usize,
    sent_bytes: usize,
    request: String,
    user_agent: String,
    ssl_cipher: String,
    ssl_protocol: String,
    target_group_arn: String,
    trace_id: String,
    domain_name: String,
    chosen_cert_arn: String,
    matched_rule_priority: u16,
    request_creation_time: String,
    actions_executed: String,
    redirect_url: String,
    error_reason: String,
}

impl Request {
    fn from_bytes(bytes: Vec<u8>) -> Option<Vec<Request>> {
        Some(
            std::str::from_utf8(&bytes)
                .ok()?
                .lines()
                .filter_map(|line| line.parse().ok())
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Deserialize, Recap)]
#[recap(regex = r#"^(?P<bucket>[^/]+)/(?P<path>.*)"#)]
struct BucketPath {
    bucket: String,
    path: String,
}

fn main() {
    let ElbLogs { bucket_path } = ElbLogs::from_args();
    let region = Region::default();
    let s3 = S3Client::new(region);
    let BucketPath { bucket, path } = bucket_path.parse().unwrap();
    let now = Utc::now();
    let until = now - chrono::Duration::minutes(15);
    let after = now - chrono::Duration::minutes(20);
    //let window = after.time()..until.time();  https://github.com/rust-lang/rust/pull/59152
    let today = now.format("%Y/%m/%d").to_string();
    let date_path = format!("{}/{}", path, today);
    // https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
    //
    // bucket[/prefix]/AWSLogs/aws-account-id/elasticloadbalancing/region/yyyy/mm/dd/aws-account-id_elasticloadbalancing_region_load-balancer-name_end-time_ip-address_random-string.log
    let objects = s3
        .list_objects_v2(ListObjectsV2Request {
            bucket: bucket.clone(),
            prefix: Some(date_path),
            ..ListObjectsV2Request::default()
        })
        .sync()
        .unwrap()
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter(move |obj| {
            DateTime::parse_from_rfc3339(obj.last_modified.clone().unwrap_or_default().as_str())
                .ok()
                .into_iter()
                .map(|parsed| parsed.time())
                .any(|last_modified| {
                    // move towards range.contains(last_modified) https://github.com/rust-lang/rust/pull/59152
                    after.time() < last_modified && last_modified < until.time()
                })
        })
        .collect::<Vec<_>>();
    println!("found {} objects", objects.len());
    let contents = objects.into_iter().map(move |object| {
        let key = object.key.unwrap_or_default();
        println!("{}", key);
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
        // let ob = s3
        // .select_object_content(SelectObjectContentRequest {
        //     bucket: bucket.clone(),
        //     key,
        //     expression_type: "CVS".into(),
        //     expression: "SELECT * FROM S3Object".into(),
        //     input_serialization: InputSerialization {
        //         csv: Some(CSVInput {
        //             field_delimiter: Some(" ".into()),
        //             ..CSVInput::default()
        //         }),
        //         compression_type: Some("GZIP".into()),
        //         ..InputSerialization::default()
        //     },
        //     output_serialization: OutputSerialization {
        //         ..OutputSerialization::default()
        //     },
        //     ..SelectObjectContentRequest::default()
        // })
        s3.get_object(GetObjectRequest {
            bucket: bucket.clone(),
            key,
            ..GetObjectRequest::default()
        })
        .map_err(Error::from)
        .and_then(|obj| {
            read_to_end(
                GzDecoder::new(obj.body.unwrap().into_async_read()),
                Vec::new(),
            )
            .map(|(_, bytes)| Request::from_bytes(bytes).unwrap_or_default())
            .map_err(Error::from)
        })
    });
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let results = rt.block_on(future::join_all(contents));
    for line in results
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
    {
        println!("{:#?}", line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn deserialize_types() -> Result<(), Box<dyn Error>> {
        assert_eq!(serde_json::from_str::<Type>(r#""https""#)?, Type::Https);
        Ok(())
    }
}
