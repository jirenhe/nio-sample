package com.wanshifu.transformers.core.channel.worker.transformer.classloader;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JarLoader extends URLClassLoader {

    public JarLoader(String libPath, ClassLoader parent) throws MalformedURLException {
        super(getURLs(libPath), parent);
    }

    private static URL[] getURLs(String path) throws MalformedURLException {
        assert null != path;

        List<String> dirs = new ArrayList<>();
        dirs.add(path);
        collectDirs(path, dirs);

        List<URL> urls = new ArrayList<>();
        for (String dir : dirs) {
            urls.addAll(doGetURLs(dir));
        }

        return urls.toArray(new URL[0]);
    }

    private static void collectDirs(String path, List<String> collector) {

        File current = new File(path);
        if (!current.exists() || !current.isDirectory()) {
            return;
        }

        for (File child : Objects.requireNonNull(current.listFiles())) {
            if (!child.isDirectory()) {
                continue;
            }

            collector.add(child.getAbsolutePath());
            collectDirs(child.getAbsolutePath(), collector);
        }
    }

    private static List<URL> doGetURLs(final String path) throws MalformedURLException {

        File jarPath = new File(path);

        /* set filter */
        FileFilter jarFilter = pathname -> pathname.getName().endsWith(".jar");

        /* iterate all jar */
        File[] allJars = new File(path).listFiles(jarFilter);
        assert allJars != null;
        List<URL> jarURLs = new ArrayList<>(allJars.length);

        for (File allJar : allJars) {
            jarURLs.add(allJar.toURI().toURL());
        }

        return jarURLs;
    }
}
