#pragma once
#include <mutex>

namespace facebook {
namespace gorilla {
extern std::mutex g_mutex;
extern uint32_t gLSN;
}
} // facebook:gorilla
