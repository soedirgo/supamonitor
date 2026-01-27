use pgrx::{
    pg_sys::{ffi::pg_guard_ffi_boundary, EnableQueryId},
    prelude::*,
};
use serde::Serialize;

pgrx::pg_module_magic!(name, version);

#[derive(Serialize)]
struct SmQueryLog<'a> {
    query: &'a str,
    calls: usize,
}

static SUPAMONITOR_VERSION: &str = env!("CARGO_PKG_VERSION");

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
        pgrx::debug1!("TODO post_parse_analyze_hook");
        if let Some(prev_hook) = PREV_POST_PARSE_ANALYZE_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse_state, query, jumble_state));
        }
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
        pgrx::debug1!("TODO planner_hook");
        if let Some(prev_hook) = PREV_PLANNER_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse, query_string, cursor_options, bound_params))
        } else {
            pg_sys::standard_planner(parse, query_string, cursor_options, bound_params)
        }
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
        pgrx::debug1!("TODO ExecutorStart_hook");
        if let Some(prev_hook) = PREV_EXECUTOR_START_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc, eflags));
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }

    // ExecutorRun_hook
    static mut PREV_EXECUTOR_RUN_HOOK: pg_sys::ExecutorRun_hook_type = None;
    PREV_EXECUTOR_RUN_HOOK = pg_sys::ExecutorRun_hook;
    pg_sys::ExecutorRun_hook = Some(executor_run_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_run_hook(
        query_desc: *mut pg_sys::QueryDesc,
        direction: pg_sys::ScanDirection::Type,
        count: u64,
        execute_once: bool,
    ) {
        pgrx::debug1!("TODO ExecutorRun_hook");
        if let Some(prev_hook) = PREV_EXECUTOR_RUN_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc, direction, count, execute_once));
        } else {
            pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once);
        }
    }

    // ExecutorFinish_hook
    static mut PREV_EXECUTOR_FINISH_HOOK: pg_sys::ExecutorFinish_hook_type = None;
    PREV_EXECUTOR_FINISH_HOOK = pg_sys::ExecutorFinish_hook;
    pg_sys::ExecutorFinish_hook = Some(executor_finish_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_finish_hook(query_desc: *mut pg_sys::QueryDesc) {
        pgrx::debug1!("TODO ExecutorFinish_hook");
        if let Some(prev_hook) = PREV_EXECUTOR_FINISH_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc));
        } else {
            pg_sys::standard_ExecutorFinish(query_desc);
        }
    }

    // ExecutorEnd_hook
    static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
    PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
    pg_sys::ExecutorEnd_hook = Some(executor_end_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
        if let Some(prev_hook) = PREV_EXECUTOR_END_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(query_desc));
        } else {
            pgrx::debug1!("TODO ExecutorEnd_hook");
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
        let query_text = std::ffi::CStr::from_ptr(query_string).to_string_lossy();
        let log_entry = SmQueryLog {
            query: &query_text,
            calls: 1,
        };
        if let Ok(json) = serde_json::to_string(&log_entry) {
            pgrx::log!("supamonitor_{SUPAMONITOR_VERSION}_log:{json}");
        }

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
    }
}
