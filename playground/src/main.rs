// ############################################################################
// Playground
// ############################################################################

use std::fs;

fn main() {
    println!("{:?}", visit_dirs());
}


// one possible implementation of walking a directory only visiting files
fn visit_dirs() -> Vec<String> {
    let mut inputs = Vec::new();
    if let Ok(read_dir) = fs::read_dir("./data/inputs") {
        for entry in read_dir {
            let path = entry.unwrap().path();
            let filename = path.file_stem().unwrap().to_str().unwrap();
            if filename != ".gitignore" {
                inputs.push(
                    format!("{:}", filename)
                );
            }
        }
    }
    inputs
}
