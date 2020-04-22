package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class DAOImpl implements DAO {
    private final SortedMap<ByteBuffer, ByteBuffer> data = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return data.tailMap(from).entrySet()
                .stream()
                .map(item -> Record.of(item.getKey(), item.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, final @NotNull ByteBuffer value) throws IOException {
        data.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
    }
}
