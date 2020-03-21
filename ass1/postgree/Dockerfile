FROM postgres:12
COPY . /postgree
RUN chown -R postgres:postgres /postgree
ADD ./init_dump.sh /docker-entrypoint-initdb.d
