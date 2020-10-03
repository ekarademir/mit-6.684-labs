// ############################################################################
// Playground
// ############################################################################

use hyper::Uri;

trait HostPort {
    fn host_port(&self) -> String;
}

impl HostPort for Uri {
    fn host_port(&self) -> String {
        let auth = self.clone().into_parts().authority.unwrap();
        format!(
            "{}:{}",
            auth.host(),
            auth.port().unwrap()
        )
    }
}

fn main () {
    // use std::time::Instant;
    // let now = Instant::now();
    // println!("Now is {:?}", now);
    // let later = Instant::now();
    // println!("Later is {:?}", later);
    // println!("passed {:?}", later.duration_since(now));




    let uri = Uri::from_static("http://master:3000");
    println!("{:?}", uri.host_port());
}
