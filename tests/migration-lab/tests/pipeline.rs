use futures::executor::block_on;
use n2x_migration_lab::Lab;

#[test]
fn sequential_versions_and_index_only_change() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        let v1 = lab.run_to("v1").await.unwrap();
        assert_eq!(v1["version"], "v1");
        assert_eq!(
            v1["tables"]["users"]["columns"],
            serde_json::json!(["id", "first_name", "last_name"])
        );
        assert_eq!(v1["files"]["a_exists"], false);
        assert_eq!(v1["files"]["b"].as_array().unwrap().len(), 4);
        let v2 = lab.run_to("v2").await.unwrap();
        assert_eq!(v2["version"], "v2");
        assert_eq!(
            v2["tables"]["users"]["columns"],
            serde_json::json!(["id", "name"])
        );
        assert_eq!(v2["tables"]["users"]["rows"][0]["name"], "Ada Lovelace");
        let v3 = lab.run_to("v3").await.unwrap();
        assert_eq!(v3["version"], "v3");
        assert_eq!(v2["tables"]["users"], v3["tables"]["users"]);
        assert!(
            v3["schema"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["name"] == "idx_users_name")
        );
        let error = lab.run_to("v1").await.unwrap_err();
        assert_eq!(error.code.0, n2x::ErrorCode::ROLLBACK_UNSUPPORTED);
        assert_eq!(lab.state().await.unwrap()["version"], "v3");
    })
}

#[test]
fn jump_to_v3_visits_every_version_and_same_target_is_noop() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        let final_state = lab.run_to("v3").await.unwrap();
        assert_eq!(final_state["version"], "v3");
        let log = std::fs::read_to_string(dir.path().join("observations.jsonl")).unwrap();
        assert!(log.contains("\"version\":\"v1\""));
        assert!(log.contains("\"version\":\"v2\""));
        assert!(log.contains("\"version\":\"v3\""));
        let noop = lab.run_to("v3").await.unwrap();
        assert_eq!(noop["run"]["status"], "Completed");
        assert_eq!(noop["tables"], final_state["tables"]);
    })
}

#[test]
fn rollback_reports_error_and_can_resume_without_reimport() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.run_to("v1").await.unwrap();
        lab.fault.set(true);
        let failed = lab.run_to("v2").await.unwrap();
        assert_eq!(failed["version"], "v1");
        assert_eq!(failed["run"]["status"], "Failed");
        assert_eq!(
            failed["run"]["last_error"]["code"],
            "lab.business.rolled_back"
        );
        assert_eq!(
            failed["tables"]["users_v2"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        lab.resume().await.unwrap();
        for _ in 0..100 {
            let state = lab.tick().await.unwrap();
            if state["run"]["status"] == "Completed" {
                break;
            }
        }
        let recovered = lab.state().await.unwrap();
        assert_eq!(recovered["version"], "v2");
        assert_eq!(
            recovered["tables"]["users"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
    })
}

#[test]
fn restart_after_external_commit_before_checkpoint_is_deduplicated() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.start("v3").await.unwrap();
        for _ in 0..100 {
            let state = lab.tick().await.unwrap();
            if state["tables"]["users"]["rows"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty())
                && state["run"]["progress"]["confirmed"]["read"] == 0
            {
                break;
            }
        }
        let mut reopened = lab.reopen().unwrap();
        let final_state = reopened.run_to("v3").await.unwrap();
        assert_eq!(final_state["version"], "v3");
        assert_eq!(
            final_state["tables"]["users"]["rows"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
    })
}

#[test]
fn restart_at_each_command_boundary_preserves_exact_results() {
    block_on(async {
        for cut in 1..90 {
            let dir = tempfile::tempdir().unwrap();
            let mut lab = Lab::open(dir.path()).unwrap();
            lab.start("v3").await.unwrap();
            for _ in 0..cut {
                let state = lab.tick().await.unwrap();
                if state["run"]["status"] == "Completed" {
                    break;
                }
            }
            let mut reopened = lab.reopen().unwrap();
            let state = reopened.run_to("v3").await.unwrap();
            assert_eq!(state["version"], "v3", "cut {cut}");
            assert_eq!(
                state["tables"]["users"]["rows"].as_array().unwrap().len(),
                4,
                "cut {cut}"
            );
            assert_eq!(state["files"]["a_exists"], false, "cut {cut}");
        }
    })
}

#[test]
fn pause_at_boundary_then_resume() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.start("v1").await.unwrap();
        lab.control(n2x::Control::Pause).unwrap();
        for _ in 0..20 {
            let state = lab.tick().await.unwrap();
            if state["run"]["status"] == "Paused" {
                break;
            }
        }
        assert_eq!(lab.state().await.unwrap()["run"]["status"], "Paused");
        lab.resume().await.unwrap();
        for _ in 0..200 {
            let state = lab.tick().await.unwrap();
            if state["run"]["status"] == "Completed" {
                break;
            }
        }
        assert_eq!(lab.state().await.unwrap()["version"], "v1");
    })
}

#[test]
fn changed_source_stops_before_version_advance() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.start("v1").await.unwrap();
        let mut reached = false;
        for _ in 0..50 {
            let state = lab.tick().await.unwrap();
            if state["run"]["progress"]["confirmed"]["read"] == 2
                && state["next_command"]["kind"]["Read"].is_object()
            {
                reached = true;
                break;
            }
        }
        assert!(reached);
        std::fs::write(dir.path().join("users.json"), "[]").unwrap();
        for _ in 0..10 {
            let state = lab.tick().await.unwrap();
            if state["run"]["status"] == "Failed" {
                break;
            }
        }
        let state = lab.state().await.unwrap();
        assert_eq!(state["version"], "v0");
        assert_eq!(
            state["run"]["last_error"]["code"],
            n2x::ErrorCode::SOURCE_CHANGED
        );
    })
}

#[test]
fn cancelled_partial_run_can_be_explicitly_resumed() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let mut lab = Lab::open(dir.path()).unwrap();
        lab.start("v1").await.unwrap();
        lab.control(n2x::Control::Cancel).unwrap();
        for _ in 0..20 {
            let state = lab.tick().await.unwrap();
            if state["run"]["status"] == "Cancelled" {
                break;
            }
        }
        assert_eq!(lab.state().await.unwrap()["run"]["status"], "Cancelled");
        lab.resume().await.unwrap();
        assert_eq!(lab.run_to("v1").await.unwrap()["version"], "v1");
    })
}
