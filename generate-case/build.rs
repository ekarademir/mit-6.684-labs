/// Loads key-value pairs in the .env file located at the root
/// to environment variables, during the build.

use std::fs;


fn main() {
    // Tell cargo to rerun this script if this file has been changed.
    println!("cargo:rerun-if-changed=.env");

    // Read the contents of the .env file, if not present pass empty string
    let environment_variables = fs::read_to_string(".env");
    let contents = match environment_variables {
        Ok(contents) => contents,
        Err(_) => String::new()
    };

    // Split the contents upon each line
    // Then for each line, split from the equal sign
    // Finally pass onto carto to set it as environment variable
    let kv_lines = contents.split("\n");
    for kv_line in kv_lines {
        let key_and_value: Vec<&str> = kv_line.trim().split('=').collect();
        if key_and_value.len() == 2 {
            println!("cargo:rustc-env={}={}", key_and_value[0].trim(), key_and_value[1].trim());
        }
    }
}
