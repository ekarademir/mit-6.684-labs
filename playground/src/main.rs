// ############################################################################
// Playground
// ############################################################################

use hyper::Uri;

fn main() {
    let uri = "http://master:3000/value?file=master_key_123".parse::<Uri>().unwrap();

    println!("{:?}", uri.path_and_query().unwrap().query().unwrap());
    println!("{:?}", uri.path_and_query().unwrap().path());
}
