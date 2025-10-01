#pragma once

#include <limestone/api/blob_pool.h>
#include <functional>
#include <atomic>
#include "limestone/api/datastore.h"
#include "blob_file_resolver.h"
#include "file_operations.h"

namespace limestone::internal {

using namespace limestone::api;

/**
 * @brief Implementation of the blob_pool interface.
 */
class blob_pool_impl : public blob_pool {
public:
    /**
     * @brief Constructs a blob_pool_impl instance with the given ID generator and blob_file_resolver.
     * @param id_generator A callable object that generates unique IDs of type blob_id_type.
     * @param resolver Reference to a blob_file_resolver instance.
     * @param datastore Reference to a datastore instance.
     */
    blob_pool_impl(std::function<blob_id_type()> id_generator, blob_file_resolver& resolver, datastore& datastore);

    void release() override;

    [[nodiscard]] blob_id_type register_file(boost::filesystem::path const& file,
                                             bool is_temporary_file) override;

    [[nodiscard]] blob_id_type register_data(std::string_view data) override;

    [[nodiscard]] blob_id_type duplicate_data(blob_id_type reference) override;

    [[nodiscard]] blob_reference_tag_type generate_reference_tag(
        blob_id_type blob_id,
        std::uint64_t transaction_id) override;

protected:
    // These protected fields and methods include:
    // - Test-specific methods
    // - Production methods that are made protected to allow for testing

    static constexpr size_t copy_buffer_size = 65536;  // Buffer size for file copy operations

    /**
     * @brief Sets a custom file_operations implementation.
     * @param file_ops A reference to the file_operations implementation.
     * 
     * Note that this function is intended for testing purposes.
     */
    void set_file_operations(file_operations& file_ops);

   /**
     * @brief Copies a file from the source path to the destination path.
     * 
     * This function uses Boost.Filesystem to copy a file from the specified source
     * path to the specified destination path. If the destination file already exists,
     * it will be overwritten.
     * 
     * @param source The path to the source file to be copied.
     * @param destination The path to the destination where the file should be copied.
     * @throws limestone_blob_exception if an I/O error occurs during the operation.
     */
    void copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination);

    /**
     * @brief Moves a file from the source path to the destination path.
     * 
     * This function uses Boost.Filesystem to move a file from the specified source
     * path to the specified destination path. If the destination file already exists,
     * it will be overwritten. The function ensures that the file move is reliable,
     * handling potential errors such as cross-filesystem moves.
     * 
     * @param source The path to the source file to be moved.
     * @param destination The path to the destination where the file should be moved.
     * @throws limestone_blob_exception if an I/O error occurs during the operation.
     */
    void move_file(const boost::filesystem::path& source, const boost::filesystem::path& destination);

    /**
     * @brief Ensures that the specified directory exists. Creates it if it does not exist.
     * @param path The directory path to check and create if necessary.
     * @throws limestone_blob_exception if the directory cannot be created.
     */
    void create_directories_if_needed(const boost::filesystem::path& path);

    /**
     * @brief Retrieves the list of blob IDs.
     * 
     * This function returns a constant reference to the list of blob IDs.
     * Note that this function is intended for testing purposes.
     * 
     * @return A constant reference to the list of blob IDs.
     */
    [[nodiscard]] std::vector<blob_id_type> get_blob_ids() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return blob_ids_;
    }

    /**
     * @brief Checks HMAC result and throws exception if failed.
     * @param result HMAC result pointer (nullptr if failed)
     */
    void handle_hmac_result(unsigned char const* result);

private:
    /**
     * @brief Generates a unique ID for a BLOB.
     *
     * @return A unique ID of type blob_id_type.
     */
    [[nodiscard]] blob_id_type generate_blob_id();

    // Callable object for ID generation
    std::function<blob_id_type()> id_generator_;

    // Reference to a blob_file_resolver instance
    blob_file_resolver& resolver_;

    // Reference to the datastore for managing BLOB data
    limestone::api::datastore& datastore_;

    // Holds the default file_operations implementation
    real_file_operations real_file_ops_;

    // Pointer to the current file_operations implementation
    file_operations* file_ops_;

    // Tracks whether the pool has been released (atomic for thread-safety)
    std::atomic<bool> is_released_{false};

    // Holds BLOB IDs used to track provisional registrations
    std::vector<blob_id_type> blob_ids_;

    // Ensures thread-safe access to blob_ids_
    std::mutex mutex_;

};

} // namespace limestone::internal
