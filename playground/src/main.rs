// ############################################################################
// Playground
// ############################################################################

use std::net::SocketAddr;
use hyper::Uri;

fn main() {


    let my_uri = "http://0.0.0.0:1234".parse::<Uri>().unwrap();
    let my_socket: SocketAddr = my_uri.into_parts().authority.unwrap().as_str().parse::<SocketAddr>().unwrap();

    println!("{:?}", my_socket);
}
