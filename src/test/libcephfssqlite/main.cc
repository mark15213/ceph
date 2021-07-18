// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>
#include <fmt/format.h>
#include "gtest/gtest.h"

#include "include/uuid.h"
#include "include/libcephfssqlite.h"

#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/ceph_time.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "client/Client.h"

#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "unittest_libcephfssqlite: "

#define sqlcatchcode(S, code) \
do {\
    rc = S;\
    if (rc != code) {\
        std::cout << "[" << __FILE__ << ":" << __LINE__ << "]"\
                  << " sqlite3 error: " << rc << " `" << sqlite3_errstr(rc)\
                  << "': " << sqlite3_errmsg(db) << std::endl;\
        sqlite3_finalize(stmt);\
        stmt = NULL;\
        goto out;\
    }\
} while (0)

#define sqlcatch(S) sqlcatchcode(S, SQLITE_OK)

static boost::intrusive_ptr<CephContext> cct;

class CephFSSQLiteTest : public ::testing::Test {
public:
    inline static const std::string fsname = "cephfssqlite";

    void SetUp() override {
        uuid.generate_random();
        ASSERT_EQ(0, db_open());
    }
    void TearDown() override {
        ASSERT_EQ(SQLITE_OK, sqlite3_close(db));
        db = nullptr;
        /* Leave database behind for inspection. */
    }

protected:
    int db_open()
    {
        static const char SQL[] =
                "PRAGMA journal_mode = PERSIST;"
                "PRAGMA page_size = 65536;"
                "PRAGMA cache_size = 32768;"
                "PRAGMA temp_store = memory;"
                "CREATE TEMPORARY TABLE perf (i INTEGER PRIMARY KEY, v TEXT);"
                "CREATE TEMPORARY VIEW p AS"
                "    SELECT perf.i, J.*"
                "    FROM perf, json_tree(perf.v) AS J;"
                "INSERT INTO perf (v)"
                "    VALUES (ceph_perf());"
        ;

        sqlite3_stmt *stmt = NULL;
        const char *current = SQL;
        int rc;

        auto&& name = get_uri();
        std::cout << "using database: " << name << std::endl;
        sqlcatch(sqlite3_open_v2(name.c_str(), &db, SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI, "cephfs"));

        std::cout << SQL << std::endl;
        sqlcatch(sqlite3_exec(db, current, NULL, NULL, NULL));

        rc = 0;
        out:
        sqlite3_finalize(stmt);
        return rc;
    }

    virtual std::string get_uri() const {
        auto uri = fmt::format("file:{}/{}?vfs=cephfs", fsname, get_name());
        return uri;
    }
    virtual std::string get_name() const {
        auto name = fmt::format("{}.db", uuid.to_string());
        return name;
    }

    sqlite3* db = nullptr;
    uuid_d uuid;
};

TEST_F(CephFSSQLiteTest, Create) {
static const char SQL[] =
        "CREATE TABLE foo (a INT);"
;

sqlite3_stmt *stmt = NULL;
const char *current = SQL;
int rc;

std::cout << SQL << std::endl;
sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
sqlcatch(sqlite3_finalize(stmt); stmt = NULL);

rc = 0;

out:
sqlite3_finalize(stmt);
ASSERT_EQ(0, rc);
}

int main(int argc, char **argv) {
    vector<const char*> args;
    argv_to_vec(argc, (const char **)argv, args);

    std::string conf_file_list;
    std::string cluster;
    CephInitParameters iparams = ceph_argparse_early_args(args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);
    cct = boost::intrusive_ptr<CephContext>(common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, 0), false);
    cct->_conf.parse_config_files(conf_file_list.empty() ? nullptr : conf_file_list.c_str(), &std::cerr, 0);
    cct->_conf.parse_env(cct->get_module_type()); // environment variables override
    cct->_conf.parse_argv(args);
    cct->_conf.apply_changes(nullptr);
    common_init_finish(cct.get());

    ldout(cct, 1) << "sqlite3 version: " << sqlite3_libversion() << dendl;
    if (int rc = sqlite3_config(SQLITE_CONFIG_URI, 1); rc) {
        lderr(cct) << "sqlite3 config failed: " << rc << dendl;
        exit(EXIT_FAILURE);
    }

    sqlite3_auto_extension((void (*)())sqlite3_cephfssqlite_init);
    sqlite3* db = nullptr;
    if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr); rc == SQLITE_OK) {
        sqlite3_close(db);
    } else {
        lderr(cct) << "could not open sqlite3: " << rc << dendl;
        exit(EXIT_FAILURE);
    }
    if (int rc = cephfssqlite_setcct(cct.get(), nullptr); rc < 0) {
        lderr(cct) << "could not set cct: " << rc << dendl;
        exit(EXIT_FAILURE);
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}