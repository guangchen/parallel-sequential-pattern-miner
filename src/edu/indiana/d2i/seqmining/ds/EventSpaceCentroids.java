package edu.indiana.d2i.seqmining.ds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cgl.imr.base.SerializationException;
import cgl.imr.base.Value;

public class EventSpaceCentroids implements Value {

	public EventSpaceCentroids(byte[] bytes) throws SerializationException {
		this.fromBytes(bytes);
	}

	public EventSpaceCentroids() {
		centroidsByType = new HashMap<Integer, Centroids>();
	}

	/**
	 * data point in 2-D space
	 * 
	 * @author Guangchen
	 * 
	 */
	public static class Point implements Value {
		private float xcoord;
		private float ycoord;

		public Point(byte[] bytes) throws SerializationException {

			this.fromBytes(bytes);
		}

		public Point(float xcoord, float ycoord) {
			super();
			this.xcoord = xcoord;
			this.ycoord = ycoord;
		}

		public float getXcoord() {
			return xcoord;
		}

		public float getYcoord() {
			return ycoord;
		}

		public void setXcoord(float xcoord) {
			this.xcoord = xcoord;
		}

		public void setYcoord(float ycoord) {
			this.ycoord = ycoord;
		}

		@Override
		public void fromBytes(byte[] bytes) throws SerializationException {
			// TODO Auto-generated method stub

			ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
			DataInputStream din = new DataInputStream(baInputStream);

			try {
				xcoord = din.readFloat();
				ycoord = din.readFloat();

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
				dout.writeFloat(xcoord);
				dout.writeFloat(ycoord);

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

	public static class ClusterCenter implements Value {
		private Point center;
		/* # of points belonging to this center */
		private long numPoints = 0L;

		public ClusterCenter(byte[] bytes) throws SerializationException {
			this.fromBytes(bytes);
		}

		public ClusterCenter(Point center) {
			this.center = center;
		}

		public ClusterCenter(Point center, long numPoints) {
			this.center = center;
			this.numPoints = numPoints;
		}

		public void addNumPoints(int count) {
			numPoints += count;
		}

		public long getNumPoints() {
			return numPoints;
		}

		public void setNumPoints(long numPoints) {
			this.numPoints = numPoints;
		}

		public void setCenter(Point p) {
			center = p;
		}

		public void setCenter(float xcoord, float ycoord) {
			/* center cannot be null as it is required field in constructor */
			center.setXcoord(xcoord);
			center.setYcoord(ycoord);

		}

		public Point getCenter() {
			return center;
		}

		@Override
		public String toString() {
			return String.format("Xcoord=%f Ycoord=%f NumPoints=%d",
					center.getXcoord(), center.getYcoord(), numPoints);
		}

		@Override
		public void fromBytes(byte[] bytes) throws SerializationException {
			// TODO Auto-generated method stub

			ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
			DataInputStream din = new DataInputStream(baInputStream);

			try {
				center = new Point(din.readFloat(), din.readFloat());
				numPoints = din.readLong();

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
				dout.writeFloat(center.getXcoord());
				dout.writeFloat(center.getYcoord());
				dout.writeLong(numPoints);

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

	/**
	 * centroids of a event type
	 * 
	 * @author Guangchen
	 * 
	 */
	public static class Centroids implements Value {
		private int eventType;
		private List<ClusterCenter> centroids;
		private boolean converged = false;

		public Centroids(byte[] bytes) throws SerializationException {
			this.fromBytes(bytes);
		}

		public Centroids(int eventType) {
			super();
			this.eventType = eventType;
			centroids = new ArrayList<ClusterCenter>();
		}

		public void addCentroid(ClusterCenter centroid) {
			centroids.add(centroid);
		}

		public boolean isConverged() {
			return converged;
		}

		public void setConverged(boolean converged) {
			this.converged = converged;
		}

		public int getEventType() {
			return eventType;
		}

		public List<ClusterCenter> getCentroids() {
			return centroids;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Event type=").append(eventType).append("\n");

			for (ClusterCenter center : centroids) {
				sb.append(center.toString()).append("\n");
			}

			return sb.toString();
		}

		@Override
		public void fromBytes(byte[] bytes) throws SerializationException {
			// TODO Auto-generated method stub

			ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
			DataInputStream din = new DataInputStream(baInputStream);

			try {
				eventType = din.readInt();
				converged = din.readBoolean();

				// read # of centroids
				int numCentroids = din.readInt();

				centroids = new ArrayList<ClusterCenter>(numCentroids);

				for (int i = 0; i < numCentroids; i++) {
					float xcoord = din.readFloat();
					float ycoord = din.readFloat();
					long numPoints = din.readLong();

					centroids.add(new ClusterCenter(new Point(xcoord, ycoord),
							numPoints));
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
				// write out eventyType
				dout.writeInt(eventType);
				// write out converged
				dout.writeBoolean(converged);

				// write out size of the list
				dout.writeInt(centroids.size());

				// write out each centroid
				for (int i = 0; i < centroids.size(); i++) {
					dout.writeFloat(centroids.get(i).getCenter().getXcoord());
					dout.writeFloat(centroids.get(i).getCenter().getYcoord());
					dout.writeLong(centroids.get(i).getNumPoints());
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

	/* mapping between event type and corresponding centroids */
	private Map<Integer, Centroids> centroidsByType = null;

	public Map<Integer, Centroids> getAllCentroids() {
		return centroidsByType;
	}

	/**
	 * get centroids of the specified event type
	 * 
	 * @param eventType
	 * @return
	 */
	public Centroids getCentroidsByType(int eventType) {
		return centroidsByType.get(eventType);
	}

	public void addCentroidsByType(int eventType, Centroids centroids) {
		centroidsByType.put(eventType, centroids);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (Map.Entry<Integer, Centroids> entry : centroidsByType.entrySet()) {
			sb.append("---Centroid---\n");
			sb.append("key (eventType) = " + entry.getKey() + "\n");
			sb.append(entry.getValue().toString());
			sb.append("---Centroid---\n");
		}

		return sb.toString();
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		// TODO Auto-generated method stub

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {

			// read # of entries
			int numEntries = din.readInt();

			centroidsByType = new HashMap<Integer, Centroids>(numEntries);

			for (int i = 0; i < numEntries; i++) {
				int eventType = din.readInt();
				int valueLength = din.readInt();

				byte[] value = new byte[valueLength];
				int numBytesRead = din.read(value);
				assert numBytesRead == valueLength;

				centroidsByType.put(eventType, new Centroids(value));
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
			// write out # of mappings
			dout.writeInt(centroidsByType.size());

			// write out each entry
			for (Map.Entry<Integer, Centroids> entry : centroidsByType
					.entrySet()) {
				// write out key
				dout.writeInt(entry.getKey());

				byte[] value = entry.getValue().getBytes();
				// write out size of value
				dout.writeInt(value.length);
				// write content of value
				dout.write(value);
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
