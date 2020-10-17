pub trait Mapper {
    fn emitIntermediate(key: &str, value: String) -> Result<(), str> {
        println!("Emitting key: {}, value: {}", key, value);
        Ok(())
    }

}
