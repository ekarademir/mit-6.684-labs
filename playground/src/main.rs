// ############################################################################
// Playground
// ############################################################################

use tokio::io::{self, AsyncWriteExt};
use tokio::fs::File;

#[tokio::main]
async fn main() {
    async fn write_intermediate(filename: &String, content: String) -> io::Result<()> {
        let path = format!("./data/intermediate/{:}.txt", filename);
        println!("Creating {:?}", path);
        let mut buffer = File::create(path).await?;
        println!("Writing buffer");
        buffer.write_all(content.as_bytes()).await?;
        println!("Done");
        Ok(())
    }
    match write_intermediate(&"file".to_string(), "some content".to_string()).await {
        Ok(_) => println!("Written"),
        Err(e) => println!("{:?}", e)
    }
}
