/*
 * Copyright 2026 Zilliz
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

import java.nio.file.Files;
import java.nio.file.Path;

/** Loads native libraries in the exact order supplied by the caller. */
public final class NativeLoadCheck {
    private NativeLoadCheck() {}

    public static void main(String[] arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("At least one absolute native library path is required");
        }
        for (String argument : arguments) {
            Path library = Path.of(argument);
            if (!library.isAbsolute()) {
                throw new IllegalArgumentException("Native library path is not absolute: " + argument);
            }
            library = library.normalize();
            if (!Files.isRegularFile(library)) {
                throw new IllegalArgumentException("Native library does not exist: " + library);
            }
            System.load(library.toString());
            System.out.println("LOADED " + library.getFileName());
        }
        System.out.println("PASS");
    }
}
