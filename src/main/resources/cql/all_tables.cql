use datastax_widerow_demo; 

CREATE TABLE followers ( 
	id text,
	follower_id uuid,
	follower_name text,
	PRIMARY KEY (id, follower_id)
) WITH compaction={'class': 'LeveledCompactionStrategy'};

truncate followers;
