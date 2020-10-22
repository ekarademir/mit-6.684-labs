// ############################################################################
// Playground
// ############################################################################

use tokio;
use tokio::sync::mpsc;


#[tokio::main]
async fn main() -> Result<(), ()> {
    let (tx, mut rx) = mpsc::channel(100);

    for i in 0..10 {
        let mut tx = tx.clone();

        tokio::spawn(async move {
            tx.send(i).await.unwrap();
        });
    }

    drop(tx);

    while let Some(res) = rx.recv().await {
        println!("{}", res);
    }

    Ok(())
}
