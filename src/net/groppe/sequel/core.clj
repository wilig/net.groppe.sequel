(ns net.groppe.sequel.core
  (:refer-clojure :exclude [filter])
  (:require [clojure.contrib.str-utils :as str-utils]))

(defn- stmt
  [& stmts]
  (if (vector? (last stmts))
    {:stmt (reduce #(str %1 %2) "" (butlast stmts)) :vars (last stmts)}
    {:stmt (reduce #(str %1 %2) "" stmts) :vars []}))

(defn- sqlize
  [kw]
  (str-utils/re-sub #"\/" "." (str-utils/re-sub #"^:" "" (str kw))))

(defn- paramatize
  [x]
  (cond
   (keyword? x) (stmt (sqlize x))
   :else (stmt "?" [x])))

(defn- sql-exp
  [op p1 p2]
  (merge-with concat (paramatize p1) (stmt op) (paramatize p2)))

(defn- nested
  [query]
  (let [q (reduce #(merge-with concat %1 %2) (stmt "(") query)]
    (merge-with concat q (stmt ")"))))

(defmacro filter
  [preds query]
  `(let [~'and (fn[& xs#] (nested (interpose (stmt " AND ") xs#)))
	 ~'or (fn[& xs#] (nested (interpose (stmt " OR ") xs#)))
	 ~'= (fn[x# y#] (sql-exp " = " x# y#))
	 ~'> (fn[x# y#] (sql-exp " > " x# y#))
	 ~'< (fn[x# y#] (sql-exp " < " x# y#))
	 ~'like (fn[x# y#] (sql-exp " like " x# y#))
	 ~'in (fn[x# xs#]
		(stmt (sqlize x#) " IN (" (apply str (interpose ", " xs#)) ")"))
	 f# (merge-with concat (stmt " WHERE ") ~preds)]
     (merge-with concat ~query f#)))

(defn collect*
  [table & xs]
  (if (vector? (last xs))
    (let [tbls (str-utils/str-join ", " (map #(sqlize %) (conj (butlast xs) table)))
	  cols (str-utils/str-join ", " (map #(sqlize %) (last xs)))]
      (stmt "select " cols " from " tbls))
    (let [tbls (str-utils/str-join ", " (map #(sqlize %) (conj xs table)))]
      (stmt "select * from " tbls))))

(defmacro collect
  [table & xs]
  `(let [~'as (fn[name# col#] (str (sqlize col#) " AS " (sqlize name#)))]
     (collect* ~table ~@xs)))

(defn limit
  [num query]
  (merge-with concat query (stmt " LIMIT " num)))

(defn offset
  [num query]
  (merge-with concat query (stmt " OFFSET " num)))

(defmacro order-by
  [& forms]
  (let [sorts (butlast forms)
	query (last forms)]
    `(let [~'asc (fn[x# & xs#] (str (str-utils/str-join ", " (map sqlize (conj xs# x#))) " ASC"))
	   ~'desc (fn[x# & xs#] (str (str-utils/str-join ", " (map sqlize (conj xs# x#))) " DESC"))]
       (let [ob# [~@sorts]]
	 (merge-with concat ~query (stmt (str " ORDER BY " (str-utils/str-join ", " ob#))))))))

(defn to-sql
  [query]
  (into [(apply str (query :stmt))] (query :vars)))