// ############################################################################
// Playground
// ############################################################################

mod inner{
    pub enum ATask {

    }
    #[macro_export]
    macro_rules! tasks {
        (
            $(
                mapper $mapper:ident() $mapper_expr: block
            )*
            $(
                reducer $reducer:ident() $reducer_expr: block
            )*
        ) => {
            $(
                fn $reducer() $reducer_expr
            )*
        };
    }
}

tasks!(
    reducer osman() {
        println!("OSMAN");
    }

    reducer orhan() {
        println!("ORHAN");
    }
);

fn main() {
    osman();
    orhan();
}
