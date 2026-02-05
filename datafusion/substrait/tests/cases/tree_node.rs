// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Tests for the TreeNode implementation for Substrait Rel

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use std::fs::File;
use std::io::BufReader;
use substrait::proto::rel::RelType;
use substrait::proto::{Plan, ProjectRel, Rel, plan_rel};

fn root_rel() -> Rel {
    let plan: Plan = serde_json::from_reader(BufReader::new(
        File::open("tests/testdata/contains_plan.substrait.json").unwrap(),
    ))
    .unwrap();
    match plan.relations[0].rel_type.clone().unwrap() {
        plan_rel::RelType::Rel(rel) => rel,
        plan_rel::RelType::Root(root) => root.input.unwrap(),
    }
}

fn kinds(rel: &Rel) -> Result<Vec<&'static str>> {
    let mut kinds = vec![];
    rel.apply(|r| {
        kinds.push(match &r.rel_type {
            Some(RelType::Project(_)) => "project",
            Some(RelType::Filter(_)) => "filter",
            Some(RelType::Read(_)) => "read",
            _ => "other",
        });
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(kinds)
}

#[test]
fn tree_visit() -> Result<()> {
    assert_eq!(kinds(&root_rel())?, ["project", "filter", "read"]);
    Ok(())
}

#[test]
fn tree_map() -> Result<()> {
    let rel = root_rel();
    let Some(RelType::Project(original)) = &rel.rel_type else {
        panic!("expected a project root");
    };
    assert!(original.common.is_some());

    let transformed = rel.clone().transform(|r| match r.rel_type {
        Some(RelType::Project(p)) => Ok(Transformed::yes(Rel {
            rel_type: Some(RelType::Project(Box::new(ProjectRel {
                common: None,
                ..*p
            }))),
        })),
        rel_type => Ok(Transformed::no(Rel { rel_type })),
    })?;

    assert!(transformed.transformed);
    let Some(RelType::Project(project)) = &transformed.data.rel_type else {
        panic!("expected a project root");
    };
    assert!(project.common.is_none());
    assert_eq!(project.input, original.input);
    assert_eq!(kinds(&transformed.data)?, ["project", "filter", "read"]);

    let unchanged = rel.clone().transform(|r| Ok(Transformed::no(r)))?;
    assert!(!unchanged.transformed);
    assert_eq!(unchanged.data, rel);
    Ok(())
}
