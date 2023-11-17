package com.prediction.king.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ListWritable<T extends Writable> implements Writable {
    private Class<T> clazz;
    private List<T> list;

    public ListWritable() {
        clazz = null;
        list = null;
    }

    public ListWritable(Class<T> clazz) {
        list = new ArrayList<>();
        this.clazz = clazz;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public int size() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public void add(T element) {
        list.add(element);
    }

    public void add(int index, T element) {
        list.add(index, element);
    }

    public T get(int index) {
        return list.get(index);
    }

    public T remove(int index) {
        return list.remove(index);
    }

    public void set(int index, T element) {
        list.set(index, element);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clazz.getName());
        out.writeInt(list.size());
        list.forEach(element -> {
            try {
                element.write(out);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            clazz = (Class<T>) Class.forName(in.readUTF());
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        int count = in.readInt();
        this.list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            try {
                T obj = clazz.newInstance();
                obj.readFields(in);
                list.add(obj);
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
