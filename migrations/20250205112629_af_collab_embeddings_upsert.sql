CREATE OR REPLACE PROCEDURE af_collab_embeddings_upsert(
    IN p_workspace_id UUID,
    IN p_oid TEXT,
    IN p_partition_key INT,
    IN p_tokens_used INT,
    IN p_fragments af_fragment_v3[]
)
    LANGUAGE plpgsql
AS
$$
BEGIN

    -- delete all the fragments not found in input fragments
    DELETE
    FROM af_collab_embeddings
    WHERE oid = p_oid
      AND partition_key = p_partition_key
      AND fragment_id NOT IN (SELECT fragment_id FROM UNNEST(p_fragments) AS f);

    MERGE INTO af_collab_embeddings AS t
    USING (SELECT f.fragment_id,
                  p_oid           AS oid,
                  p_partition_key AS partition_key,
                  f.content_type,
                  f.contents,
                  f.embedding,
                  NOW(),
                  f.metadata,
                  f.fragment_index,
                  f.embedder_type
           FROM UNNEST(p_fragments) AS f) AS s
    ON t.oid = s.oid AND
       t.partition_key = s.partition_key AND
       t.fragment_id = s.fragment_id
    WHEN MATCHED THEN -- this fragment has not changed
        UPDATE SET indexed_at = now()
    WHEN NOT MATCHED THEN -- this fragment is new
        INSERT
        VALUES (s.fragment_id, s.oid, s.partition_key, s.content_type, s.contents, s.embedding, now(),
                s.metadata, s.fragment_index, s.embedder_type);

    -- Update the usage tracking table
    INSERT INTO af_workspace_ai_usage(created_at, workspace_id, search_requests, search_tokens_consumed,
                                      index_tokens_consumed)
    VALUES (now()::date, p_workspace_id, 0, 0, p_tokens_used)
    ON CONFLICT (created_at, workspace_id)
        DO UPDATE SET index_tokens_consumed = af_workspace_ai_usage.index_tokens_consumed + p_tokens_used;
END
$$;