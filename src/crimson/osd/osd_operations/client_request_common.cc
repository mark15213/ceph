// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

typename InterruptibleOperation::template interruptible_future<>
CommonClientRequest::do_recover_missing(
  Ref<PG>& pg, const hobject_t& soid)
{
  eversion_t ver;
  logger().debug("{} check for recovery, {}", __func__, soid);
  if (!pg->is_unreadable_object(soid, &ver) &&
      !pg->is_degraded_or_backfilling_object(soid)) {
    return seastar::now();
  }
  logger().debug("{} need to wait for recovery, {}", __func__, soid);
  if (pg->get_recovery_backend()->is_recovering(soid)) {
    return pg->get_recovery_backend()->get_recovering(soid).wait_for_recovered();
  } else {
    auto [op, fut] =
      pg->get_shard_services().start_operation<UrgentRecovery>(
        soid, ver, pg, pg->get_shard_services(), pg->get_osdmap_epoch());
    return std::move(fut);
  }
}

} // namespace crimson::osd
