create table if not exists "user"
(
	id serial not null
		constraint user_pk
			primary key,
	user_name text not null,
	average_rate double precision
);

alter table "user" owner to postgres;

create unique index if not exists user_id_uindex
	on "user" (id);

create table if not exists item
(
	id serial not null
		constraint item_pk
			primary key,
	item_name text not null,
	average_rate double precision
);

alter table item owner to postgres;

create unique index if not exists item_id_uindex
	on item (id);

create table if not exists bill
(
	id serial not null
		constraint bill_pk
			primary key,
	user_id integer not null
		constraint bill_user_id_fk
			references "user",
	item_id integer not null
		constraint bill_item_id_fk
			references item,
	rate double precision not null
);

alter table bill owner to postgres;

create unique index if not exists bill_id_uindex
	on bill (id);

