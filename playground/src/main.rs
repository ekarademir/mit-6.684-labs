// ############################################################################
// Playground
// ############################################################################
use petgraph;

fn main() {
    let mut myg: petgraph::Graph<&str, (), petgraph::Directed> = petgraph::Graph::new();
    let node1 = myg.add_node("node1");
    let node2 = myg.add_node("node2");

    println!("Node 1: {:?}", myg[node1]);
}
