use log::debug;
use serde::{Deserialize, Serialize};

use super::pipeline::TaskNode;

// TODO Probably a macro to register a task which,
//    * Copies the function
//      * Either converts function_case to CamelCase or gets also ATask name
//    * Adds an entry in the match assignment
// Tried this a bit but this is very hard to pull of as intended. So for not this is inconvenient
// but fast to implement


#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskInput {
    pub machine_addr: String,
    pub file: String,
}

pub type TaskInputs = Vec<TaskInput>;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ATask {
    CountWords,
    SumCounts,
}

impl ATask {
    pub fn is_map(&self) -> bool {
        match self {
            ATask::CountWords => true,
            _ => false
        }
    }

    pub fn is_reduce(&self) -> bool {
        !self.is_map()
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: ATask,
    pub input: TaskInputs,
    pub task_id: u32,
}


impl TaskAssignment {
    pub async fn execute(&self) {
        // TODO Pull the contents of the input and feed them into the functions
        match self.task {
            ATask::CountWords => count_words(&self.input).await,
            ATask::SumCounts => sum_counts(&self.input).await,
        }
    }
}

async fn count_words(input: &TaskInputs) {
    debug!("Counting words!!!!");
}

async fn sum_counts(input: &TaskInputs) {
    debug!("Total Count!!!!");
}


#[cfg(test)]
mod tests {
  #[test]
  fn test_map_reduce_test() {
    let atask = super::ATask::CountWords;
    assert_eq!(atask.is_map(), true);
    assert_eq!(atask.is_reduce(), false);
  }
}
