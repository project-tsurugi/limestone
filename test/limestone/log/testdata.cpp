#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"
#include "testdata.h"

namespace limestone::testing {


std::string data_manifest(int persistent_format_version) {
    std::ostringstream ss;
    ss << "{ \"format_version\": \"1.0\", \"persistent_format_version\": " << persistent_format_version << " }";
    return ss.str();
}

void create_file(const boost::filesystem::path& path, std::string_view content) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::out | std::ios_base::binary);
    strm.write(content.data(), content.size());
    strm.flush();
    LOG_IF(FATAL, !strm || !strm.is_open() || strm.bad() || strm.fail());
    strm.close();
}

std::string read_entire_file(const boost::filesystem::path& path) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::in | std::ios_base::binary);
    assert(strm.good());
    std::ostringstream ss;
    ss << strm.rdbuf();
    strm.close();
    return ss.str();
}

} // namespace limestone::testing
