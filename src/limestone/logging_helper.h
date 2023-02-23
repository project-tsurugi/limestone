/*
 * Copyright 2023-2023 tsurugi project.
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

#include <array>
#include <string_view>

namespace limestone {

// NOLINTNEXTLINE
#define AUTO_LOCATION_PREFIX_VERSION 2

// location prefix ver.1

constexpr std::string_view find_fullname(std::string_view prettyname, std::string_view funcname) {
    // search (funcname + "(")
    size_t fn_pos = 0;  // head of function name
    while ((fn_pos = prettyname.find(funcname, fn_pos)) != std::string_view::npos) {
        if (prettyname[fn_pos + funcname.size()] == '(') {
            break;  // found
        }
        fn_pos++;
    }
    if (fn_pos == std::string_view::npos) {
        return funcname;  // fallback
    }
    // search to left, but skip <...>
    size_t start_pos = std::string_view::npos;
    int tv_nest = 0;  // "<...>" nest level
    for (int i = fn_pos; i >= 0; i--) {
        switch (prettyname[i]) {
            case '>': tv_nest++; continue;
            case '<': tv_nest--; continue;
            case ' ': if (tv_nest <= 0) start_pos = i+1; break;
        }
        if (start_pos != std::string_view::npos) {
            break;
        }
    }
    if (start_pos == std::string_view::npos) {  // no return type, such as constructors
        start_pos = 0;
    }
    return std::string_view(prettyname.data() + start_pos, fn_pos + funcname.length() - start_pos);
}

template<size_t N>
constexpr auto location_prefix(const std::string_view sv) {
    std::array<char, N + 3> buf{};

    // tsurugi logging location prefix:
    // * "::" -> ":"
    // * "<...>" -> ""
    // * [-A-Za-z0-9_:] only
    int p = 0;
    buf.at(p++) = '/';
    buf.at(p++) = ':';
    int tv_nest = 0;  // "<...>" nest level
    for (size_t i = 0; i < sv.size(); i++) {
        if (sv[i] == '<') {
            tv_nest++;
        } else if (sv[i] == '>') {
            tv_nest--;
        } else {
            if (tv_nest <= 0) {
                if (sv[i] == ':' && sv[i + 1] == ':') {
                    buf.at(p++) = ':';
                    i++;  // skip second colon
                } else {
                    buf.at(p++) = sv[i];
                }
            }
        }
    }
    buf.at(p++) = ' ';
    buf.at(p) = 0;
    return buf;
}

template<size_t N, size_t M>
constexpr auto location_prefix(const char (&prettyname)[N], const char (&funcname)[M]) {  // NOLINT
    const std::string_view sv = find_fullname(prettyname, funcname);  // NOLINT
    return location_prefix<std::max(N, M)>(sv);
}

// location prefix ver.2

constexpr size_t skip_paren(std::string_view a) {
    const char leftc = a.at(0);
    const char rightc = leftc == '(' ? ')' : leftc == '<' ? '>' : leftc == '[' ? ']' : throw std::runtime_error("unimplemented paren type");
    for (size_t i = 1; i < a.length(); i++) {
        auto c = a.at(i);
        if (c == '(' || c == '<' || c == '[') {  // nest
            i += skip_paren(std::string_view{a.data()+i, a.length()-i});
            continue;
        }
        if (c == ')' || c == '>' || c == ']') {
            if (c == rightc) {
                // returns offset of right paren
                return i;
            }
            throw std::runtime_error("paren mismatch");
        }
        if (a.find("operator", i) == i) {
            // need special care for operator in template value??
            // such as "<operator<<, 123>", permit in C++20 or later??
            throw std::runtime_error("operator in paren not supported");
        }
    }
    throw std::runtime_error("reach end in paren");
}

template<size_t N>
constexpr void array_appendstr(std::array<char, N>& buf, size_t &off, const char *str) {
    for (char c : std::string_view(str)) {
        buf.at(off++) = c;
    }
//std::cout << "DBG: " << str << std::endl;
}

template<size_t N>
constexpr auto shrink_prettyname(const char (&prettyname)[N]) {  // NOLINT
    std::array<char, N> buf{}; // store funcname
    std::string_view pn{prettyname, N-1};  // NOLINT
    bool funcname_found = false;  // function-name is found
    size_t buf_off = 0;
//try{
    for (size_t i = 0; i < pn.length(); i++) {
        auto c = pn.at(i);
        if (c == ' ') {
            // skip tailing 'const' and '[...]'

            if (pn.find(" const", i) == i) {
                if (pn.length() == i + 6 || pn.at(i + 6) == ' ') {
                    i += 6-1; // skip " const"
                    continue;
                }
            }
            if (pn.at(i + 1) == '[') {
                i++;
                i += skip_paren(std::string_view{pn.data() + i, pn.length() - i});
                continue;
            }
            if (funcname_found) {
                throw std::runtime_error("unexpected tailing space char");
            }
            // currently buffered string was typename, so discard
            buf_off = 0;  // buf reset
            continue;
        }
        if (pn.find("::<lambda(", i) == i) {
            // g++ closure, form: "::<lambda(...)>"
            i += 9;
            i += skip_paren(std::string_view{pn.data() + i, pn.length() - i});
            if (pn.at(++i) != '>') {
                throw std::runtime_error("right '>' error");
            }
            i++;
            array_appendstr(buf, buf_off, ":lambda");
            if (pn.length() == i) {  // if tail, this is function name
                funcname_found = true;
            }
            continue;
        }
        if (pn.find("::(anonymous class)::operator()", i) == i) {
            // clang closure
            array_appendstr(buf, buf_off, ":lambda");
            i += 31-1;
            continue;
        }
        if (c == '(' || c == '<' || c == '[') {
            i += skip_paren(std::string_view{pn.data() + i, pn.length() - i});
            if (c == '(') {
                funcname_found = true;
            }
            continue;
        }
        if (c == ')' || c == '>' || c == ']') {
            throw std::runtime_error("paren mismatch");
        }
        if (c == ':') {
            if (pn.at(++i) == ':') {
                buf.at(buf_off++) = ':';
//std::cout << "DBG: :" << std::endl;
            }
            // else
            //     TODO: parse error
            continue;
        }
        if (pn.find("operator", i) == i) {
            i += 8;  // "operator"
            // this implementation simply drop symbols, i.e. any operatorXX -> "operator"
            array_appendstr(buf, buf_off, "operator");
            // g++9: operatorXX(...) const [...]
            // g++10: operatorXX<...>
            auto co1 = pn.at(i);  // op 1st char
            // find the end of operator-name,
            // skip to '('
            //         or (for g++10) '<'
            if (co1 == '(') { // special case: "operator()" contains '('
                if (pn.at(i+1) == ')') {
                    i+=2;
                } else {
                    throw std::runtime_error("unknown operator(X");
                }
            } else if (co1 == '<') {  // (g++10 workaround) special cases: "operator<", "operator<<", "operator<<=", "operator<=>"
                i++;  // skip '<'
                if (pn.at(i) == '<') {
                    i++;  // skip "<<"
                }
            }
            while (pn.at(i) != '(' && pn.at(i) != '<') {
                i++;
            }
            // reached end of operator-name
            i--;
        } else {
            buf.at(buf_off++) = c;
//std::cout << "DBG: " << c << std::endl;
        }
    }
    if (!funcname_found) { throw std::runtime_error("parse error"); }
//} catch(const char *p) { std::cout << p << std::endl; }

    buf.at(buf_off++) = 0;
    return buf;
}

template<size_t N>
constexpr size_t shrinked_length(const char (&prettyname)[N]) {  // NOLINT
    auto buf = shrink_prettyname(prettyname); // store funcname
    return std::string_view{buf.data()}.size();
}

template<size_t N>
constexpr auto location_prefix_v2(const char (&prettyname)[N]) {  // NOLINT
    auto buf = shrink_prettyname(prettyname); // store funcname
    //constexpr size_t n = shrinked_length(prettyname) + 1;  // +1: '\0'
    constexpr size_t n = N;
    std::array<char, n+3> ret{'/', ':'};
    size_t i = 0;
    for (; buf.at(i) != 0; i++) {
        ret.at(i+2) = buf.at(i);
    }
    ret.at(i+2) = ' ';
    ret.at(i+3) = 0;
    return ret;
}

// To force compile-time evaluation of constexpr location_prefix in C++17,
// the result is once stored to the temporary constexpr variable.

// NOTE: restriction of this implementation
// side effect: If used in the form:
//    if (cond) LOG_LP(ERROR) << "message";
// your C++ compiler may warn that this statement contains dangling-else.
//
// workaround: rewrite like:
//    if (cond) { LOG_LP(ERROR) << "message"; }

// N.B. use consteval in C++20

#if AUTO_LOCATION_PREFIX_VERSION == 2
// NOLINTNEXTLINE
#define _LOCATION_PREFIX_TO_STREAM(stream)  if (constexpr auto __tmplp = location_prefix_v2(__PRETTY_FUNCTION__); false) {} else stream << __tmplp.data()
#else
// NOLINTNEXTLINE
#define _LOCATION_PREFIX_TO_STREAM(stream)  if (constexpr auto __tmplp = location_prefix(__PRETTY_FUNCTION__, __FUNCTION__); false) {} else stream << __tmplp.data()
#endif

// NOLINTNEXTLINE
#define LOG_LP(x)   _LOCATION_PREFIX_TO_STREAM(LOG(x))
// NOLINTNEXTLINE
#define VLOG_LP(x)  _LOCATION_PREFIX_TO_STREAM(VLOG(x))
// NOLINTNEXTLINE
#define DVLOG_LP(x) _LOCATION_PREFIX_TO_STREAM(DVLOG(x))

} // namespace
