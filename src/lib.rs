use pgrx::{
    pg_sys::{
        ffi::pg_guard_ffi_boundary, CleanQuerytext, EnableQueryId, InstrAlloc, InstrEndLoop,
        InstrumentOption::INSTRUMENT_ALL,
    },
    prelude::*,
};
use serde::Serialize;
use std::time::Instant;

pgrx::pg_module_magic!(name, version);
#[derive(Serialize)]

struct SmQueryLog<'a> {
    query_id: i64,
    query: &'a str,
    calls: usize,
    total_plan_time: f64,
    total_exec_time: f64,
}

static SUPAMONITOR_VERSION: &str = env!("CARGO_PKG_VERSION");

// We prevent double counting of optimizable statements in utility statements by
// clearing the queryId outside of executor hooks. Tracking will be skipped in
// executor hooks.
// TODO: normalize query
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    if !pgrx::pg_sys::process_shared_preload_libraries_in_progress {
        pgrx::error!("this extension must be loaded via shared_preload_libraries.");
    }
    EnableQueryId();

    // post_parse_analyze_hook
    static mut PREV_POST_PARSE_ANALYZE_HOOK: pg_sys::post_parse_analyze_hook_type = None;
    PREV_POST_PARSE_ANALYZE_HOOK = pg_sys::post_parse_analyze_hook;
    pg_sys::post_parse_analyze_hook = Some(post_parse_analyze_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn post_parse_analyze_hook(
        parse_state: *mut pg_sys::ParseState,
        query: *mut pg_sys::Query,
        jumble_state: *mut pg_sys::JumbleState,
    ) {
        if let Some(prev_hook) = PREV_POST_PARSE_ANALYZE_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse_state, query, jumble_state));
        }

        let utility_stmt = (*query).utilityStmt;
        if !utility_stmt.is_null()
            && matches!((*utility_stmt).type_, pg_sys::NodeTag::T_ExecuteStmt)
        {
            (*query).queryId = 0i64 as u64;
            return;
        }

        // TODO: track jumbled query if needed
    }

    // planner_hook
    static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(planner_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn planner_hook(
        parse: *mut pg_sys::Query,
        query_string: *const std::ffi::c_char,
        cursor_options: std::ffi::c_int,
        bound_params: *mut pg_sys::ParamListInfoData,
    ) -> *mut pg_sys::PlannedStmt {
        // Can't process query w/o query_string
        let track_planning = !query_string.is_null() && (*parse).queryId != 0i64 as u64;
        let now = Instant::now();

        let result = if let Some(prev_hook) = PREV_PLANNER_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse, query_string, cursor_options, bound_params))
        } else {
            pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
        };

        if track_planning {
            let elapsed = now.elapsed();
            let query_id = (*parse).queryId;
            let mut stmt_location = (*result).stmt_location;
            let mut stmt_len = (*result).stmt_len;

            let query_text = CleanQuerytext(query_string, &mut stmt_location, &mut stmt_len);
            let query_text = std::ffi::CStr::from_ptr(query_text)
                .to_str()
                .unwrap_or("<invalid utf8>");

            let log_entry = SmQueryLog {
                query_id: query_id as i64,
                query: &query_text,
                calls: 0,
                total_plan_time: elapsed.as_secs_f64() * 1000.0,
                total_exec_time: 0.0,
            };
            if let Ok(json) = serde_json::to_string(&log_entry) {
                pgrx::log!("supamonitor_{SUPAMONITOR_VERSION}_log:{json}");
            }
        }

        result
    }

    // ExecutorStart_hook
    static mut PREV_EXECUTOR_START_HOOK: pg_sys::ExecutorStart_hook_type = None;
    PREV_EXECUTOR_START_HOOK = pg_sys::ExecutorStart_hook;
    pg_sys::ExecutorStart_hook = Some(executor_start_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_start_hook(
        query_desc: *mut pg_sys::QueryDesc,
        eflags: std::ffi::c_int,
    ) {
        if let Some(prev_hook) = PREV_EXECUTOR_START_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc, eflags));
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }

        if (*(*query_desc).plannedstmt).queryId == 0i64 as u64 {
            return;
        }

        if (*query_desc).totaltime.is_null() {
            let oldctx = pg_sys::MemoryContextSwitchTo((*(*query_desc).estate).es_query_cxt);
            (*query_desc).totaltime = InstrAlloc(1, INSTRUMENT_ALL as i32, false);
            pg_sys::MemoryContextSwitchTo(oldctx);
        }
    }

    // // ExecutorRun_hook
    // static mut PREV_EXECUTOR_RUN_HOOK: pg_sys::ExecutorRun_hook_type = None;
    // PREV_EXECUTOR_RUN_HOOK = pg_sys::ExecutorRun_hook;
    // pg_sys::ExecutorRun_hook = Some(executor_run_hook);

    // #[pg_guard]
    // unsafe extern "C-unwind" fn executor_run_hook(
    //     query_desc: *mut pg_sys::QueryDesc,
    //     direction: pg_sys::ScanDirection::Type,
    //     count: u64,
    //     execute_once: bool,
    // ) {
    //     if let Some(prev_hook) = PREV_EXECUTOR_RUN_HOOK {
    //         pg_guard_ffi_boundary(|| prev_hook(query_desc, direction, count, execute_once));
    //     } else {
    //         pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once);
    //     }
    // }

    // // ExecutorFinish_hook
    // static mut PREV_EXECUTOR_FINISH_HOOK: pg_sys::ExecutorFinish_hook_type = None;
    // PREV_EXECUTOR_FINISH_HOOK = pg_sys::ExecutorFinish_hook;
    // pg_sys::ExecutorFinish_hook = Some(executor_finish_hook);

    // #[pg_guard]
    // unsafe extern "C-unwind" fn executor_finish_hook(query_desc: *mut pg_sys::QueryDesc) {
    //     if let Some(prev_hook) = PREV_EXECUTOR_FINISH_HOOK {
    //         pg_guard_ffi_boundary(|| prev_hook(query_desc));
    //     } else {
    //         pg_sys::standard_ExecutorFinish(query_desc);
    //     }
    // }

    // ExecutorEnd_hook
    static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
    PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
    pg_sys::ExecutorEnd_hook = Some(executor_end_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
        let query_id = (*(*query_desc).plannedstmt).queryId;

        if query_id != 0i64 as u64 && !(*query_desc).totaltime.is_null() {
            InstrEndLoop((*query_desc).totaltime);

            let query_string = (*query_desc).sourceText;
            if !query_string.is_null() {
                let mut stmt_location = (*(*query_desc).plannedstmt).stmt_location;
                let mut stmt_len = (*(*query_desc).plannedstmt).stmt_len;

                let query_text = CleanQuerytext(query_string, &mut stmt_location, &mut stmt_len);
                let query_text = std::ffi::CStr::from_ptr(query_text)
                    .to_str()
                    .unwrap_or("<invalid utf8>");

                let log_entry = SmQueryLog {
                    query_id: query_id as i64,
                    query: &query_text,
                    calls: 1,
                    total_plan_time: 0.0,
                    total_exec_time: (*(*query_desc).totaltime).total * 1000.0,
                };
                if let Ok(json) = serde_json::to_string(&log_entry) {
                    pgrx::log!("supamonitor_{SUPAMONITOR_VERSION}_log:{json}");
                }
            }
        }

        if let Some(prev_hook) = PREV_EXECUTOR_END_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc));
        } else {
            pg_sys::standard_ExecutorEnd(query_desc);
        }
    }

    // ProcessUtility_hook
    static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;
    PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
    pg_sys::ProcessUtility_hook = Some(process_utility_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn process_utility_hook(
        pstmt: *mut pg_sys::PlannedStmt,
        query_string: *const std::ffi::c_char,
        read_only_tree: bool,
        context: pg_sys::ProcessUtilityContext::Type,
        params: *mut pg_sys::ParamListInfoData,
        query_env: *mut pg_sys::QueryEnvironment,
        dest: *mut pg_sys::DestReceiver,
        qc: *mut pg_sys::QueryCompletion,
    ) {
        let utility_stmt = (*pstmt).utilityStmt;
        let query_id = (*pstmt).queryId;
        (*pstmt).queryId = 0i64 as u64;
        let mut stmt_location = (*pstmt).stmt_location;
        let mut stmt_len = (*pstmt).stmt_len;

        // Skip logging for ExecuteStmt, PrepareStmt, and DeallocateStmt
        let should_skip = !utility_stmt.is_null()
            && matches!(
                (*utility_stmt).type_,
                pg_sys::NodeTag::T_ExecuteStmt
                    | pg_sys::NodeTag::T_PrepareStmt
                    | pg_sys::NodeTag::T_DeallocateStmt
            );
        if should_skip {
            if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
                pg_guard_ffi_boundary(|| {
                    prev_hook(
                        pstmt,
                        query_string,
                        read_only_tree,
                        context,
                        params,
                        query_env,
                        dest,
                        qc,
                    )
                });
            } else {
                pg_sys::standard_ProcessUtility(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    qc,
                );
            }
            return;
        }

        let now = Instant::now();

        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            pg_guard_ffi_boundary(|| {
                prev_hook(
                    pstmt,
                    query_string,
                    read_only_tree,
                    context,
                    params,
                    query_env,
                    dest,
                    qc,
                )
            });
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }

        let elapsed = now.elapsed();

        let query_text = CleanQuerytext(query_string, &mut stmt_location, &mut stmt_len);
        let query_text = std::ffi::CStr::from_ptr(query_text)
            .to_str()
            .unwrap_or("<invalid utf8>");

        let log_entry = SmQueryLog {
            query_id: query_id as i64,
            query: &query_text,
            calls: 1,
            total_plan_time: 0.0,
            total_exec_time: elapsed.as_secs_f64() * 1000.0,
        };
        if let Ok(json) = serde_json::to_string(&log_entry) {
            pgrx::log!("supamonitor_{SUPAMONITOR_VERSION}_log:{json}");
        }
    }
}
