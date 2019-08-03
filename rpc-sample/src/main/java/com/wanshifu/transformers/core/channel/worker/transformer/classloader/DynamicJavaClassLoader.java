package com.wanshifu.transformers.core.channel.worker.transformer.classloader;

import com.sun.org.apache.xalan.internal.xsltc.compiler.CompilerException;

import javax.tools.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.CharBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DynamicJavaClassLoader extends URLClassLoader {

    private final ConcurrentMap<String, byte[]> classBytes = new ConcurrentHashMap<>();

    private final String sourcePath;

    private final String libPath;

    public DynamicJavaClassLoader(String sourcePath, String libPath) throws IOException, CompilerException {
        super(new URL[0], mathParent(libPath));
        this.sourcePath = sourcePath;
        this.libPath = libPath;
        this.classBytes.putAll(compile());
    }

    private static ClassLoader mathParent(String libPath) throws MalformedURLException {
        ClassLoader parent = new URLClassLoader(new URL[0], DynamicGroovyClassLoader.class.getClassLoader());
        if (libPath != null) {
            parent = new JarLoader(libPath, parent);
        }
        return parent;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] buf = classBytes.remove(name);
        if (buf != null) {
            return defineClass(name, buf, 0, buf.length);
        } else {
            throw new ClassNotFoundException("class " + name + " not found!");
        }
    }

    private Map<String, byte[]> compile() throws IOException, CompilerException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager stdManager = compiler.getStandardFileManager(null, null, null);
        MemoryJavaFileManager manager = new MemoryJavaFileManager(stdManager);
        File file = new File(sourcePath);
        List<File> files = new ArrayList<>();
        getSourceFiles(file, files);
        List<JavaFileObject> javaFileObjects = new ArrayList<>(files.size());
        for (File file1 : files) {
            javaFileObjects.add(MemoryJavaFileManager.makeStringSource(file1.getName(), new String(readBytes(file1))));
        }
        Iterable<String> options = Arrays.asList("-encoding", "utf-8", "-classpath", getCompileClasspath(), "-sourcepath", sourcePath);
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        JavaCompiler.CompilationTask task = compiler.getTask(null, manager, diagnostics, options, null, javaFileObjects);
        if (task.call()) {
            return manager.getClassBytes();
        } else {
            StringBuilder sb = new StringBuilder();
            diagnostics.getDiagnostics().forEach(diagnostic -> sb.append(diagnostic.getMessage(Locale.CHINA)));
            throw new CompilerException(sb.toString());
        }
    }

    private String getCompileClasspath() {
        URLClassLoader urlClassLoader = (URLClassLoader) this.getParent();
        StringBuilder jars = new StringBuilder(System.getProperty("java.class.path")).append(";");
        Arrays.stream(urlClassLoader.getURLs()).forEach(url -> jars.append(url.getPath()).append(";"));
        return jars.toString();
    }

    private static byte[] readBytes(File file) throws IOException {
        byte[] bytes = new byte[(int) file.length()];
        FileInputStream fileInputStream = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fileInputStream);
        try {
            dis.readFully(bytes);
            InputStream temp = dis;
            dis = null;
            temp.close();
        } finally {
            if (dis != null) {
                dis.close();
            }
        }
        return bytes;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getLibPath() {
        return libPath;
    }

    private static void getSourceFiles(File sourceFile, List<File> sourceFileList) {
        if (sourceFile.exists() && sourceFileList != null) {
            if (sourceFile.isDirectory()) {
                File[] childrenFiles = sourceFile.listFiles(pathname -> {
                    if (pathname.isDirectory()) {
                        return true;
                    } else {
                        return pathname.getName().endsWith(".java");
                    }
                });
                // 递归调用
                if (childrenFiles != null) {
                    for (File childFile : childrenFiles) {
                        getSourceFiles(childFile, sourceFileList);
                    }
                }
            } else {
                sourceFileList.add(sourceFile);
            }
        }
    }

    /**
     * JavaFileManager that keeps compiled .class bytes in memory.
     */
    @SuppressWarnings("unchecked")
    final static class MemoryJavaFileManager extends ForwardingJavaFileManager {
        /**
         * Java source file extension.
         */
        private final static String EXT = ".java";
        private Map<String, byte[]> classBytes;

        public MemoryJavaFileManager(JavaFileManager fileManager) {
            super(fileManager);
            classBytes = new HashMap<>();
        }

        public Map<String, byte[]> getClassBytes() {
            return classBytes;
        }

        public void close() {
            classBytes = new HashMap<>();
        }

        public void flush() {
        }

        /**
         * A file object used to represent Java source coming from a string.
         */
        private static class StringInputBuffer extends SimpleJavaFileObject {
            final String code;

            StringInputBuffer(String name, String code) {
                super(toURI(name), Kind.SOURCE);
                this.code = code;
            }

            public CharBuffer getCharContent(boolean ignoreEncodingErrors) {
                return CharBuffer.wrap(code);
            }

            public Reader openReader() {
                return new StringReader(code);
            }
        }

        /**
         * A file object that stores Java bytecode into the classBytes map.
         */
        private class ClassOutputBuffer extends SimpleJavaFileObject {
            private final String name;

            ClassOutputBuffer(String name) {
                super(toURI(name), Kind.CLASS);
                this.name = name;
            }

            public OutputStream openOutputStream() {
                return new FilterOutputStream(new ByteArrayOutputStream()) {
                    public void close() throws IOException {
                        out.close();
                        ByteArrayOutputStream bos = (ByteArrayOutputStream) out;
                        classBytes.put(name, bos.toByteArray());
                    }
                };
            }
        }

        public JavaFileObject getJavaFileForOutput(JavaFileManager.Location location,
                                                   String className,
                                                   JavaFileObject.Kind kind,
                                                   FileObject sibling) throws IOException {
            if (kind == JavaFileObject.Kind.CLASS) {
                return new ClassOutputBuffer(className);
            } else {
                return super.getJavaFileForOutput(location, className, kind, sibling);
            }
        }

        static JavaFileObject makeStringSource(String name, String code) {
            return new StringInputBuffer(name, code);
        }

        static URI toURI(String name) {
            File file = new File(name);
            if (file.exists()) {
                return file.toURI();
            } else {
                try {
                    final StringBuilder newUri = new StringBuilder();
                    newUri.append("mfm:///");
                    newUri.append(name.replace('.', '/'));
                    if (name.endsWith(EXT)) newUri.replace(newUri.length() - EXT.length(), newUri.length(), EXT);
                    return URI.create(newUri.toString());
                } catch (Exception exp) {
                    return URI.create("mfm:///com/sun/script/java/java_source");
                }
            }
        }
    }

}
