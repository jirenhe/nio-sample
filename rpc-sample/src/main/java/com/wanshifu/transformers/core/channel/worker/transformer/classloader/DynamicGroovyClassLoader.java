package com.wanshifu.transformers.core.channel.worker.transformer.classloader;

import groovy.lang.GroovyClassLoader;

import java.net.MalformedURLException;

public class DynamicGroovyClassLoader extends GroovyClassLoader {

    private final String sourcePath;

    private final String libPath;

    public DynamicGroovyClassLoader(String sourcePath, String libPath) throws MalformedURLException {
        super(mathParent(libPath));
        this.addClasspath(sourcePath);
        this.sourcePath = sourcePath;
        this.libPath = libPath;
    }

    private static ClassLoader mathParent(String libPath) throws MalformedURLException {
        ClassLoader parent = DynamicGroovyClassLoader.class.getClassLoader();
        if (libPath != null) {
            parent = new JarLoader(libPath, parent);
        }
        return parent;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getLibPath() {
        return libPath;
    }
}
