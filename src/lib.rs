use pgrx::{pg_sys::ffi::pg_guard_ffi_boundary, prelude::*};

::pgrx::pg_module_magic!(name, version);

fn delete_must_have_a_where(query: PgBox<pg_sys::Query>) {
    if query.commandType != pg_sys::CmdType::CMD_DELETE {
        return ();
    }

    let jointree = unsafe { PgBox::from_pg(query.jointree) };
    if jointree.quals.is_null() {
        panic!("DELETE queries must have a WHERE clause");
    }
}

#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    static mut PREV_POST_PARSE_ANALYZE_HOOK: pg_sys::post_parse_analyze_hook_type = None;
    PREV_POST_PARSE_ANALYZE_HOOK = pg_sys::post_parse_analyze_hook;
    pg_sys::post_parse_analyze_hook = Some(post_parse_analyze_hook);

    #[pg_guard]
    unsafe extern "C-unwind" fn post_parse_analyze_hook(
        parse_state: *mut pg_sys::ParseState,
        query: *mut pg_sys::Query,
        jumble_state: *mut pg_sys::JumbleState,
    ) {
        delete_must_have_a_where(PgBox::from_pg(query));
        if let Some(prev_hook) = PREV_POST_PARSE_ANALYZE_HOOK {
            pg_guard_ffi_boundary(|| prev_hook(parse_state, query, jumble_state));
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_delete_with_where() {
        Spi::run(
            "
            CREATE TABLE t AS SELECT 1 AS one;
            DELETE FROM t WHERE 0 = 0;
            ",
        )
        .unwrap();
        let result: Option<i64> = Spi::get_one("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(result.unwrap(), 0);
    }

    #[pg_test(error = "DELETE queries must have a WHERE clause")]
    fn test_delete_without_where() {
        Spi::run(
            "
            CREATE TABLE t AS SELECT 1 AS one;
            DELETE FROM t;
            ",
        )
        .unwrap();
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
