pub mod task_assignment;
pub mod pipeline;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    Finished,
    Failed,
    Started,
    Running,
    Error,
    CantAssign,
}

pub use task_assignment::{
    ATask,
    TaskAssignment,
    TaskInput,
};
