// ############################################################################
// Playground
// ############################################################################

use petgraph::{self, visit};

fn main() {
    let mut my_graph: petgraph::Graph<&str, (), petgraph::Directed> = petgraph::Graph::new();

    let el_0 = my_graph.add_node("level_0");
    let el_1_0 = my_graph.add_node("level_1_0");
    let el_1_1 = my_graph.add_node("level_1_1");
    let edge_01_0 = my_graph.add_edge(el_0, el_1_0, ());
    let edge_01_1 = my_graph.add_edge(el_0, el_1_1, ());
    let el_2 = my_graph.add_node("level_2");
    let edge_21_0 = my_graph.add_edge(el_1_0, el_2, ());
    let edge_21_1 = my_graph.add_edge(el_1_1, el_2, ());

    println!("{:?}", my_graph);

    let mut bfs = visit::Bfs::new(&my_graph, el_0);
    while let Some(node_idx) = bfs.next(&my_graph) {
        println!("{:?}", my_graph[node_idx]);
    }
}
