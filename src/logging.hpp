#pragma once

#include <memory>
#include <spdlog/spdlog.h>

namespace gcsplugin {
namespace logging {

const std::shared_ptr<spdlog::logger> &getLogger();

} // namespace logging
} // namespace gcsplugin