// ############################################################################
// Playground
// ############################################################################

fn main() {
    use std::collections::HashMap;

    let mut counts: HashMap<String, u128> = HashMap::new();

    let input = String::from("Osman orhan. osman");
    for word in input.split(' ') {
        let counter = counts.entry(
            word
                .to_lowercase()
                .to_string()
        ).or_insert(0);
        *counter += 1;
    }
    let coco = counts.iter()
        .map(|(word, count)| {
            (word.clone(), count.to_string())
        })
        .collect::<Vec<(String, String)>>();

    println!("{:?}", coco);

    let coco = vec![
        "2","4"
    ];

    let mut count: i128 = 0;
    for val in coco {
        count += val.parse::<i128>().unwrap_or_default();
    }

    println!("{:?}", count);

}
