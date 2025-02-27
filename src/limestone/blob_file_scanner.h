/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 #pragma once

 #include <boost/filesystem.hpp>
 #include <iterator>
 
 namespace limestone::internal {
 
 class blob_file_resolver;
 
 /**
  * @brief The blob_file_scanner class is responsible for scanning directories
  *        to find BLOB files and providing an iterator interface to traverse them.
  *
  * This class allows traversal of a given directory and its subdirectories,
  * identifying files that conform to a specified naming convention or other
  * criteria defined by the user. It provides an iterator to access these files.
  */
 class blob_file_scanner {
 public:
     /**
      * @brief Constructor.
      * @param resolver The blob_file_resolver instance to use for file resolution.
      */
     explicit blob_file_scanner(const blob_file_resolver& resolver);

     /**
      * @brief Iterator class to traverse the identified BLOB files.
      *
      * This nested class provides functionality to iterate over the BLOB files
      * found by the scanner. It adheres to the InputIterator requirements.
      */
     class iterator {
     public:
         // Iterator traits
         using iterator_category = std::input_iterator_tag;
         using value_type = boost::filesystem::path;
         using difference_type = std::ptrdiff_t;
         using pointer = const boost::filesystem::path*;
         using reference = const boost::filesystem::path&;

         /**
          * @brief Default constructor for the iterator.
          *
          * Constructs an end iterator.
          */
         iterator();

         /**
          * @brief Constructor with parameters.
          * @param iter The recursive directory iterator pointing to the current file.
          * @param resolver Pointer to the blob_file_resolver for file validation.
          */
         iterator(boost::filesystem::recursive_directory_iterator iter, const blob_file_resolver* resolver);

         /**
          * @brief Pre-increment operator to move to the next BLOB file.
          * @return Reference to the incremented iterator.
          */
         iterator& operator++();

         /**
          * @brief Dereference operator to access the current file path.
          * @return The path of the current BLOB file.
          */
         reference operator*() const;

         /**
          * @brief Inequality operator to compare iterators.
          * @param other The iterator to compare with.
          * @return True if iterators are not equal, false otherwise.
          */
         bool operator!=(const iterator& other) const;

     private:
         boost::filesystem::recursive_directory_iterator iter_;  ///< Directory iterator.
         const blob_file_resolver* resolver_;                    ///< Pointer to the resolver for file validation.

         /**
          * @brief Advances the iterator to the next valid BLOB file.
          *
          * Skips over non-regular files and files that do not match the BLOB file criteria.
          */
         void skip_non_blob_files();
     };

     /**
      * @brief Returns an iterator to the beginning of the BLOB files sequence.
      * @return An iterator pointing to the first valid BLOB file.
      */
     [[nodiscard]] iterator begin() const;

     /**
      * @brief Returns an iterator to the end of the BLOB files sequence.
      * @return An iterator representing the end of the sequence.
      */
     [[nodiscard]] iterator end() const;

 private:
     const blob_file_resolver& resolver_;  ///< Reference to the blob_file_resolver instance.
 };

 } // namespace limestone::internal
 