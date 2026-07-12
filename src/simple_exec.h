#pragma once

#include <string>

#include "common/context.h"
#include "execution/execution_manager.h"
#include "system/sm.h"

bool try_execute_simple_sql(const std::string &sql, SmManager *sm_manager, QlManager *ql_manager,
                            Context *context);
