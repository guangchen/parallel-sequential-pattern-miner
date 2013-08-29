package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class FigureSet implements Value {
	private List<PatternQueryFigure> figures;

	public FigureSet() {
		figures = new ArrayList<PatternQueryFigure>();
	}

	public FigureSet(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public List<PatternQueryFigure> getFigures() {
		return figures;
	}

	public void setFigures(List<PatternQueryFigure> figures) {
		this.figures = figures;
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			/* read patterns */
			int size = din.readInt();

			figures = new ArrayList<PatternQueryFigure>(size);

			for (int i = 0; i < size; i++) {
				int bytesLength = din.readInt();
				byte[] bytesArray = new byte[bytesLength];
				din.read(bytesArray);

				figures.add(new PatternQueryFigure(bytesArray));
			}

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
			/* write out figures */

			// write out # of figures
			dout.writeInt(figures.size());

			// write out each figure
			for (int i = 0; i < figures.size(); i++) {
				byte[] bytes = figures.get(i).getBytes();

				// write size
				dout.writeInt(bytes.length);

				// write value
				dout.write(bytes);
			}

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
