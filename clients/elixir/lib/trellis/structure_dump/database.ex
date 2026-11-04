defmodule Trellis.StructureDump.Database do
  @moduledoc false
  # Whether a repo and the `:trellis` url under its key name the same
  # database. The structure file is `pg_dump` of the repo's database plus the
  # definitions of the Trellis instance at the url: with two databases, the
  # file's tables and definitions disagree and the load fails. (The Ruby
  # client's `verify_same_database!` does the same for Rails.)
  #
  # A database is its host, port and database name, each resolved the way
  # its client resolves it. The repo's is what `mix ecto.dump` hands
  # `pg_dump` (`:socket_dir`, else `:hostname`, else `PGHOST`, else
  # `localhost`; `:port`, else `PGPORT`, else 5432; `:database`, else
  # `PGDATABASE`, else the user's name). Trellis's is read from the url alone
  # (port 5432, database the user's name), since it reads nothing from the
  # environment. A repo connecting over a socket directory matches a local
  # Trellis host (`localhost`, `127.0.0.1`, `::1` or a socket directory).
  # Other spellings of a host are not equated: `localhost` is not
  # `127.0.0.1`.
  #
  # A url this can't read, or one naming several hosts, is not compared:
  # `Trellis.connect/1` reports an unreadable url, and a failover list has
  # no single host to compare.

  @local_hosts ["localhost", "127.0.0.1", "::1"]

  @doc """
  `:ok` when the repo's `config` (`repo.config()`) and the `:trellis`
  `options` name the same database, or the comparison can't be made;
  otherwise `{:error, message}`, which never quotes the url.
  """
  @spec verify(module(), keyword(), keyword()) :: :ok | {:error, String.t()}
  def verify(repo, config, options) do
    with url when is_binary(url) <- options[:url],
         {:ok, trellis} <- trellis(url) do
      ours = repo(config)

      if same?(trellis, ours) do
        :ok
      else
        {:error,
         "#{inspect(repo)}'s :trellis url points at #{describe(trellis)}, but the repo is " <>
           "#{describe(ours)}: the definitions of one database don't belong in another's " <>
           "structure file. Point the url at the same host, port and database (spelled " <>
           "identically, e.g. `localhost` is not `127.0.0.1`)"}
      end
    else
      _ -> :ok
    end
  end

  # {host, port, database} of the repo, as `pg_dump` gets them from `mix
  # ecto.dump`'s arguments and libpq's environment.
  @doc false
  def repo(config) do
    host = config[:socket_dir] || config[:hostname] || System.get_env("PGHOST") || "localhost"
    port = present(config[:port]) || present(System.get_env("PGPORT")) || "5432"

    user =
      present(config[:username]) || present(System.get_env("PGUSER")) ||
        present(System.get_env("USER"))

    database =
      present(config[:database]) || present(System.get_env("PGDATABASE")) || user

    {to_string(host), port, database}
  end

  # {:ok, {host, port, database}} of a Trellis url (a libpq connection string
  # or a postgres:// URI); :error when it can't be read or names several
  # hosts. A database that can't be told (no name, no user) is nil.
  @doc false
  def trellis(url) do
    with {:ok, keywords} <- keywords(url),
         host when is_binary(host) <- present(keywords["host"]) || present(keywords["hostaddr"]),
         false <- String.contains?(host, ",") do
      user = present(keywords["user"]) || present(System.get_env("USER"))
      {:ok, {host, present(keywords["port"]) || "5432", present(keywords["dbname"]) || user}}
    else
      _ -> :error
    end
  end

  defp same?({host, port, database}, {repo_host, repo_port, repo_database}) do
    port == repo_port and
      (is_nil(database) or is_nil(repo_database) or database == repo_database) and
      (host == repo_host or (socket?(repo_host) and local?(host)))
  end

  defp socket?(host), do: String.starts_with?(host, "/")
  defp local?(host), do: host in @local_hosts or String.starts_with?(host, ["/", "@"])

  defp describe({host, port, database}),
    do: "#{database || "the user's database"} on #{host}:#{port}"

  defp present(nil), do: nil
  defp present(""), do: nil
  defp present(value), do: to_string(value)

  # The keywords of a libpq connection string or URI, as a map of strings.
  @doc false
  def keywords(url) do
    url = String.trim(url)

    if Regex.match?(~r{\Apostgres(?:ql)?://}, url), do: uri(url), else: key_values(url, %{})
  end

  @uri ~r{\Apostgres(?:ql)?://(?:(?<userinfo>[^@/?#]*)@)?(?<hosts>[^/?#]*)(?:/(?<path>[^?#]*))?(?:\?(?<query>[^#]*))?}

  defp uri(url) do
    %{"userinfo" => userinfo, "hosts" => hosts, "path" => path, "query" => query} =
      Regex.named_captures(@uri, url)

    {host, port} = authority(hosts)

    base =
      %{
        "user" => userinfo |> String.split(":", parts: 2) |> hd() |> decode(),
        "host" => host,
        "port" => port,
        "dbname" => decode(path)
      }
      |> Enum.reject(fn {_, value} -> value in [nil, ""] end)
      |> Map.new()

    try do
      {:ok, Map.merge(base, URI.decode_query(query))}
    rescue
      _ -> :error
    end
  end

  # "host:port", "[::1]:port", or several of them separated by commas (the
  # whole text is then the host, which `trellis/1` declines to compare).
  defp authority(hosts) do
    cond do
      String.contains?(hosts, ",") ->
        {hosts, nil}

      match?("[" <> _, hosts) ->
        case Regex.run(~r/\A\[([^\]]*)\](?::(\d*))?\z/, hosts) do
          [_, host] -> {host, nil}
          [_, host, port] -> {host, port}
          _ -> {nil, nil}
        end

      true ->
        case String.split(hosts, ":", parts: 2) do
          [host, port] -> {decode(host), port}
          [host] -> {decode(host), nil}
        end
    end
  end

  defp decode(text) do
    URI.decode(text)
  rescue
    _ -> text
  end

  # `key = value` pairs separated by white space; a value may be single
  # quoted, and a backslash escapes the next character in either form.
  defp key_values(text, acc) do
    text = String.trim_leading(text)

    if text == "" do
      {:ok, acc}
    else
      with [key, rest] <-
             Regex.run(~r/\A([^\s=]+)\s*=\s*(.*)\z/s, text, capture: :all_but_first),
           {:ok, value, rest} <- value(rest) do
        key_values(rest, Map.put(acc, key, value))
      else
        _ -> :error
      end
    end
  end

  defp value("'" <> rest), do: quoted(rest, [])
  defp value(rest), do: bare(rest, [])

  defp quoted("'" <> rest, acc), do: {:ok, finish(acc), rest}
  defp quoted("\\" <> <<char::utf8, rest::binary>>, acc), do: quoted(rest, [<<char::utf8>> | acc])
  defp quoted(<<char::utf8, rest::binary>>, acc), do: quoted(rest, [<<char::utf8>> | acc])
  defp quoted("", _), do: :error

  defp bare("\\" <> <<char::utf8, rest::binary>>, acc), do: bare(rest, [<<char::utf8>> | acc])

  defp bare(<<char::utf8, rest::binary>> = text, acc) do
    if char in [?\s, ?\t, ?\n, ?\r],
      do: {:ok, finish(acc), text},
      else: bare(rest, [<<char::utf8>> | acc])
  end

  defp bare("", acc), do: {:ok, finish(acc), ""}

  defp finish(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()
end
