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

// use std::collections::HashMap;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
struct Mine {
    comp: String,
    field: String,
}

impl PartialEq for Mine {
    fn eq(&self, other: &Self) -> bool {
        self.comp == other.comp
    }
}

impl Eq for Mine {}

impl Hash for Mine {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.comp.hash(state);
    }
}

fn main () {
    // use std::time::Instant;
    // let now = Instant::now();
    // println!("Now is {:?}", now);
    // let later = Instant::now();
    // println!("Later is {:?}", later);
    // println!("passed {:?}", later.duration_since(now));


    // let uri = Uri::from_static("http://master:3000");
    // println!("{:?}", uri.host_port());

    // let mut map: HashMap<String, String> = HashMap::new();

    // println!("Begin {:?}", map);
    // map.insert("first".to_string(), "first item".to_string());
    // println!("First insert {:?}", map);
    // map.insert("second".to_string(), "second item".to_string());
    // println!("Second insert {:?}", map);
    // map.insert("first".to_string(), "first item again".to_string());
    // println!("Third insert {:?}", map);


    let mut set: HashSet<Mine> = HashSet::new();
    println!("Begin {:?}", set);
    set.replace(
        Mine {
            comp: "first".to_string(),
            field: "First item to insert".to_string(),
        }
    );
    println!("First insert {:?}", set);
    set.replace(
        Mine {
            comp: "second".to_string(),
            field: "Second item to insert".to_string(),
        }
    );
    println!("Second insert {:?}", set);
    set.replace(
        Mine {
            comp: "first".to_string(),
            field: "Inserting to first item again".to_string(),
        }
    );
    println!("Third insert {:?}", set);

}
