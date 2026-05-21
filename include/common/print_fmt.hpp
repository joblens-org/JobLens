/* Copyright 2026 - 2026 wzycc
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
 * limitations under the License. */
#pragma once

#include <sstream>
#include <iomanip>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

inline std::string wordWrap(const std::string& text,
                            size_t width = 78,
                            const std::string& indent = ""){
    std::istringstream in(text);
    std::ostringstream out;
    std::string line, word;
    while (in >> word) {
        if (line.size() + word.size() + 1 > width) {
            out << indent << line << '\n';
            line = word;
        } else {
            if (!line.empty()) line += ' ';
            line += word;
        }
    }
    if (!line.empty()) out << indent << line << '\n';
    return out.str();
}

inline std::string wordWrapAlign(const std::string& text,
                                 size_t width,
                                 const std::string& firstPrefix,
                                 const std::string& nextPrefix)
{
    std::istringstream in(text);
    std::ostringstream out;
    std::string line, word;
    bool first = true;
    while (in >> word) {
        if (line.size() + word.size() + 1 >
            (first ? width - firstPrefix.size() : width - nextPrefix.size()))
        {
            out << (first ? firstPrefix : nextPrefix) << line << '\n';
            line = word;
            first = false;
        } else {
            if (!line.empty()) line += ' ';
            line += word;
        }
    }
    if (!line.empty()) out << (first ? firstPrefix : nextPrefix) << line << '\n';
    return out.str();
}

inline int getTerminalWidth(){
#ifdef _WIN32
    CONSOLE_SCREEN_BUFFER_INFO csbi;
    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi))
        return csbi.srWindow.Right - csbi.srWindow.Left + 1;
#else
    struct winsize w;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0)
        return w.ws_col;
#endif
    return -1;
}

