// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::time::Duration;

use datafusion_common::exec_err;
use datafusion_physical_plan::stream::ReceiverStreamBuilder;
use futures::{StreamExt, TryStreamExt, future};
use tokio::sync::oneshot;
use tokio::time::timeout;

#[tokio::test]
async fn drains_final_queue_after_successful_task_completion() {
    let mut builder = ReceiverStreamBuilder::<usize>::new(3);
    let tx = builder.tx();
    let (finished_tx, finished_rx) = oneshot::channel();
    builder.spawn(async move {
        for item in 0..3 {
            tx.send(Ok(item)).await.unwrap();
        }
        drop(tx);
        finished_tx.send(()).unwrap();
        Ok(())
    });
    let stream = builder.build();

    // The producer finishes before the stream is ever polled.
    timeout(Duration::from_secs(5), finished_rx)
        .await
        .unwrap()
        .unwrap();
    let items: Vec<_> = timeout(Duration::from_secs(5), stream.try_collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(items, vec![0, 1, 2]);
}

#[tokio::test]
async fn propagates_task_error_without_sending_it_to_the_channel() {
    let mut builder = ReceiverStreamBuilder::<usize>::new(1);
    let tx = builder.tx();
    builder.spawn(async { exec_err!("producer failed") });
    let mut stream = builder.build();

    // Keep an empty channel open: only the task result can report this error.
    let error = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.strip_backtrace(), "Execution error: producer failed");

    // A task error does not terminate the channel side of the stream.
    tx.send(Ok(42)).await.unwrap();
    drop(tx);
    assert_eq!(
        timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        42
    );
    assert!(
        timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[should_panic(expected = "receiver producer panic")]
async fn resumes_task_panic_in_stream_consumer() {
    let mut builder = ReceiverStreamBuilder::<usize>::new(1);
    builder.spawn(async { panic!("receiver producer panic") });
    let mut stream = builder.build();
    let _ = timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_unpolled_stream_aborts_pending_task() {
    let mut builder = ReceiverStreamBuilder::<usize>::new(1);
    let tx = builder.tx();
    let (started_tx, started_rx) = oneshot::channel();
    let (finished_tx, finished_rx) = oneshot::channel();
    builder.spawn(async move {
        started_tx.send(()).unwrap();
        future::pending::<()>().await;
        finished_tx.send(()).unwrap();
        Ok(())
    });
    let stream = builder.build();
    timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();

    drop(stream);

    assert!(tx.is_closed());
    // Aborting the pending future drops its completion sender without sending.
    assert!(
        timeout(Duration::from_secs(5), finished_rx)
            .await
            .unwrap()
            .is_err()
    );
}
