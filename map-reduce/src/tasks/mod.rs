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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum FinishReportStatus {
    Commited,
    Error,
}

pub use task_assignment::{
    ATask,
    TaskAssignment,
    TaskInput,
    TaskInputs,
    FinishedTask,
};

pub use pipeline::{
    Pipeline,
    NextTask
};
