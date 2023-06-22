
#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <limestone/logging.h>

#include <glog/logging.h>

DEFINE_int32(recover_max_pararelism, 1, "# of insert thread");

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    limestone::api::configuration conf({argv[1]}, "");
    conf.set_recover_max_pararelism(FLAGS_recover_max_pararelism);
    limestone::api::datastore d(conf);
    d.ready();
}
