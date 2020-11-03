use log::debug;
use serde::{Deserialize, Serialize};

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
    FinalTask,
}

#[derive(PartialEq, Debug)]
pub enum TaskKind {
    Map,
    Reduce,
    Final,
}

impl ATask {
    pub fn kind(&self) -> TaskKind {
        match self {
            ATask::CountWords => TaskKind::Map,
            ATask::SumCounts => TaskKind::Reduce,
            ATask::FinalTask => TaskKind::Final,
        }
    }

    #[allow(dead_code)] // For testing
    fn is_map(&self) -> bool {
        self.kind() == TaskKind::Map
    }

    #[allow(dead_code)] // For testing
    fn is_reduce(&self) -> bool {
        self.kind() == TaskKind::Reduce
    }
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: ATask,
    pub input: TaskInputs,
    pub task_id: u32,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResultPair {
    pub key: String,
    pub input: TaskInput
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinishedTask {
    pub task: ATask,
    pub finished: TaskInputs,
    pub task_id: u32,
    pub key: String,
    pub result: Vec<ResultPair>,
}

// TODO Find a better serialization method for results, maybe Generics
pub type TaskResult = Vec<(String, String)>;

impl TaskAssignment {
    pub async fn execute(&self) -> TaskResult {
        // TODO Pull the contents of the input and feed them into the functions
        match self.task {
            ATask::CountWords => count_words(&self.input).await,
            ATask::SumCounts => sum_counts(&self.input).await,
            _ => TaskResult::new(),
        }
    }
}

async fn count_words(input: &TaskInputs) -> TaskResult {
    debug!("Count words! {:?}", input);
    vec![
        ("word".to_string(), "42".to_string()),
        ("test".to_string(), "32".to_string()),
    ]
}

async fn sum_counts(input: &TaskInputs) -> TaskResult {
    debug!("Sum counts! {:?}", input);
    vec![
        ("wordreduced".to_string(), "seda".to_string()),
        ("testreduced".to_string(), "am".to_string()),
    ]
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
