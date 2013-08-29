package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class PatternQueryFigure implements Value {
	private String figureName;
	private byte[] content;

	public PatternQueryFigure(String figureName, byte[] content) {
		super();
		this.figureName = figureName;
		this.content = content;
	}

	public PatternQueryFigure(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public String getFigureName() {
		return figureName;
	}

	public byte[] getContent() {
		return content;
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {

			/* read figure name */
			int size = din.readInt();
			byte[] byteArray = new byte[size];
			din.read(byteArray);
			figureName = new String(byteArray);

			/* read figure */
			size = din.readInt();
			content = new byte[size];
			din.read(content);

			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	public byte[] getBytes() throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		try {

			/* write out figure name */
			byte[] name = figureName.getBytes();
			dout.writeInt(name.length);
			dout.write(name);

			/* write out figure */
			dout.writeInt(content.length);
			dout.write(content);

			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout.close();
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}

		return marshalledBytes;
	}
}
