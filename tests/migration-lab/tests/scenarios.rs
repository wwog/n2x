//! Readable end-to-end cases for the documentation examples.
//!
//! Each test is named after a guide scenario. They use the same public
//! `Lab` API as the web server, so the snippets in `site/` stay close to
//! executable code.

use futures::executor::block_on;
use n2x_migration_lab::Lab;
use serde_json::json;

#[test]
fn scenario_simple_single_edge() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        assert_eq!(lab.state().await.unwrap()["version"], "v0");

        let state = lab.run_to("v1").await.unwrap();
        assert_eq!(state["version"], "v1");
        assert_eq!(state["sql_version"], 1);
        assert_eq!(
            state["tables"]["users"]["columns"],
            json!(["id", "first_name", "last_name"])
        );
        assert_eq!(
            state["tables"]["users"]["rows"].as_array().unwrap().len(),
            4
        );
        assert_eq!(state["files"]["a_exists"], false);
        assert_eq!(state["files"]["b"].as_array().unwrap().len(), 4);
    })
}

#[test]
fn scenario_tick_pause_resume() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.start("v1").await.unwrap();
        assert!(lab.state().await.unwrap()["next_command"].is_object());

        // One business Step, not the whole edge.
        let after_step = lab.step().await.unwrap();
        assert_eq!(after_step["version"], "v0");

        lab.control(n2x::Control::Pause).unwrap();
        let mut state = lab.state().await.unwrap();
        while state["run"]["status"] != "Paused" {
            state = lab.tick().await.unwrap();
        }
        assert_eq!(state["version"], "v0");

        lab.resume().await.unwrap();
        let mut state = lab.state().await.unwrap();
        while state["run"]["status"] != "Completed" {
            state = lab.tick().await.unwrap();
        }
        assert_eq!(state["version"], "v1");
    })
}

#[test]
fn scenario_fault_and_recovery() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.run_to("v1").await.unwrap();

        // The next v2 write transaction is rolled back once.
        lab.fault.set(true);
        let failed = lab.run_to("v2").await.unwrap();
        assert_eq!(failed["version"], "v1");
        assert_eq!(failed["run"]["status"], "Failed");
        assert_eq!(
            failed["run"]["last_error"]["code"],
            "lab.business.rolled_back"
        );
        assert!(
            failed["tables"]["users_v2"]["rows"]
                .as_array()
                .unwrap()
                .is_empty()
        );

        lab.resume().await.unwrap();
        let recovered = lab.run_to("v2").await.unwrap();
        assert_eq!(recovered["version"], "v2");
        assert_eq!(
            recovered["tables"]["users"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            4
        );

        // Discard the executor and reopen the same state directory.
        let mut reopened = lab.reopen().unwrap();
        let v3 = reopened.run_to("v3").await.unwrap();
        assert_eq!(v3["version"], "v3");
        assert!(
            v3["schema"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["name"] == "idx_users_name")
        );
        assert_eq!(
            reopened.run_to("v1").await.unwrap_err().code.0,
            n2x::ErrorCode::ROLLBACK_UNSUPPORTED
        );
    })
}

#[test]
fn scenario_web_backend_flow() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();

        // GET /api/state
        assert_eq!(lab.state().await.unwrap()["version"], "v0");

        // POST /api/start {"target":"v1"}
        lab.start("v1").await.unwrap();
        // POST /api/step
        lab.step().await.unwrap();

        // POST /api/reopen: discard the executor, keep the state directory.
        let mut lab = lab.reopen().unwrap();
        assert_eq!(lab.state().await.unwrap()["executor_loaded"], false);

        // Recovers the active v1 run before accepting a new target.
        let v1 = lab.run_to("v1").await.unwrap();
        assert_eq!(v1["version"], "v1");

        // POST /api/fault
        lab.fault.set(true);
        assert_eq!(lab.state().await.unwrap()["fault_armed"], true);

        // The next v2 write fails; the run stays at v1.
        let failed = lab.run_to("v2").await.unwrap();
        assert_eq!(failed["version"], "v1");
        assert_eq!(failed["run"]["status"], "Failed");

        // POST /api/resume
        lab.resume().await.unwrap();
        let done = lab.run_to("v2").await.unwrap();
        assert_eq!(done["version"], "v2");

        // POST /api/reset: the server removes the data directory.
        let root = lab.root.clone();
        drop(lab);
        std::fs::remove_dir_all(&root).unwrap();
        let mut fresh = Lab::open(root).unwrap();
        assert_eq!(fresh.state().await.unwrap()["version"], "v0");
    })
}
