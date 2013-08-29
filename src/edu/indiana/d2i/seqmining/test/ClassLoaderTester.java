package edu.indiana.d2i.seqmining.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import cgl.imr.util.JarClassLoaderException;

public class ClassLoaderTester extends ClassLoader {

	private void populateClasses(String directory)
			throws JarClassLoaderException {
		Hashtable<String, Class<?>> classes = new Hashtable<String, Class<?>>();

		File dir = new File(directory);
		File[] jars = dir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isFile() && file.getName().endsWith(".jar");
			}
		});

		if (jars == null) {
			System.out.println("No jars");
			return;
		}

		byte classBytes[];
		Class<?> result = null;
		String className = null;
		JarInputStream jarFile;
		InputStream classInputStream;
		try {
			JarFile jar = null;
			for (File f : jars) {
				jar = new JarFile(directory + "/" + f.getName());

				jarFile = new JarInputStream(new FileInputStream(jar.getName()));
				JarEntry jarEntry;

				while (true) {
					jarEntry = jarFile.getNextJarEntry();
					if (jarEntry == null) {
						break;
					}
					if ((jarEntry.getName().endsWith(".class"))) {
						className = jarEntry.getName().replaceAll("/", "\\.")
								.replace(".class", "");

						classInputStream = jar.getInputStream(jarEntry);
						ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
						int nextValue = classInputStream.read();
						while (-1 != nextValue) {
							byteStream.write(nextValue);
							nextValue = classInputStream.read();
						}

						classBytes = byteStream.toByteArray();
						byteStream.close();
						System.out.println("Preparing loading class "
								+ className);
						result = defineClass(className, classBytes, 0,
								classBytes.length, null);
						classes.put(className, result);
						System.out.println("Loaded class " + className);
						classInputStream.close();
					}
				}
				jarFile.close();
				jar.close();
			}
		} catch (Exception e) {
			throw new JarClassLoaderException(e);
		}
	}

	public static void main(String[] args) {
		ClassLoaderTester tester = new ClassLoaderTester();
		String directory = "build\\jar";
		try {
			tester.populateClasses(directory);
		} catch (JarClassLoaderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
