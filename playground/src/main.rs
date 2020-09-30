// ############################################################################
// Playground
// ############################################################################
use std::time::Instant;

fn main () {
    let now = Instant::now();
    println!("Now is {:?}", now);
    let later = Instant::now();
    println!("Later is {:?}", later);
    println!("passed {:?}", later.duration_since(now));
}
