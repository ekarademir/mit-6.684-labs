// ############################################################################
// Playground
// ############################################################################

// use hyper::Uri;

// trait HostPort {
//     fn host_port(&self) -> String;
// }

// impl HostPort for Uri {
//     fn host_port(&self) -> String {
//         let auth = self.clone().into_parts().authority.unwrap();
//         format!(
//             "{}:{}",
//             auth.host(),
//             auth.port().unwrap()
//         )
//     }
// }

use std::collections::HashMap;

fn main () {
    // use std::time::Instant;
    // let now = Instant::now();
    // println!("Now is {:?}", now);
    // let later = Instant::now();
    // println!("Later is {:?}", later);
    // println!("passed {:?}", later.duration_since(now));


    // let uri = Uri::from_static("http://master:3000");
    // println!("{:?}", uri.host_port());

    let mut map: HashMap<String, String> = HashMap::new();

    println!("Begin {:?}", map);
    map.insert("first".to_string(), "first item".to_string());
    println!("First insert {:?}", map);
    map.insert("second".to_string(), "second item".to_string());
    println!("Second insert {:?}", map);
    map.insert("first".to_string(), "first item again".to_string());
    println!("Third insert {:?}", map);

}
