namespace Joiner

open System.Collections.Generic

module Joins =

    let hashJoin (A:'a seq) (B:'b seq) (projectionA:'a ->'c) (projectionB:'b -> 'c):('a * 'b) seq =
        let lengthA:int = Seq.length A
        let lengthB:int = Seq.length B
        match lengthA < lengthB with
        | true ->
            let dictionaryA:Dictionary<'c,'a> = Dictionary<'c,'a>(lengthA)
            Seq.iter (fun (a:'a) -> dictionaryA.[projectionA a] <- a) A
            Seq.map (fun (b:'b) -> (dictionaryA.[projectionB b], b)) B
        | false ->
            let dictionaryB:Dictionary<'c,'b> = Dictionary<'c,'b>(lengthB)
            Seq.iter (fun (b:'b) -> dictionaryB.[projectionB b] <- b) B
            Seq.map (fun (a:'a) -> (a, dictionaryB.[projectionA a])) A

    let mergeJoin (A:'a seq) (B:'b seq) (projectionA:'a ->'c)  (projectionB:'b -> 'c):('a * 'b) seq =
        let sortedA:'a list =
            A
            |> Seq.toList
            |> List.sortBy projectionA
        let sortedB:'b list =
            B
            |> Seq.toList
            |> List.sortBy projectionB
        let rec merger (listA:'a list) (listB:'b list):('a * 'b) list =
            match listA, listB with
            | [], _ -> []
            | _, [] -> []
            | (headA:'a)::(tailA:'a list), (headB:'b)::(tailB:'b list) ->
                let projectedA:'c = projectionA headA
                let projectedB:'c = projectionB headB
                match projectedA, projectedB with
                | projectedA, projectedB when projectedA = projectedB -> (headA, headB)::(merger tailA tailB)
                | projectedA, projectedB when projectedA < projectedB -> merger tailA listB
                | projectedA, projectedB when projectedA > projectedB -> merger listA tailB
        merger sortedA sortedB
        |> List.toSeq

    let nestedJoin (A:'a seq) (B:'b seq) (predicate:'a ->'b -> bool):('a * 'b) seq =
        Seq.allPairs A B
        |> Seq.filter (fun (a:'a, b:'b) -> predicate a b)

    let equiJoinHash (A:'a seq) (B:'b seq) (projectionA:'a ->'c) (projectionB:'b -> 'c):('a * 'b) seq = hashJoin A B projectionA projectionB

    let equiJoinMerge (A:'a seq) (B:'b seq) (projectionA:'a ->'c) (projectionB:'b -> 'c):('a * 'b) seq = mergeJoin A B projectionA projectionB

    let innerJoin (A:'a seq) (B:'b seq) (predicate:'a ->'b -> bool):('a * 'b) seq = nestedJoin A B predicate

    let leftOuterJoin (A:'a seq) (B:'b seq) (predicate:'a ->'b -> bool):('a * 'b option) seq =
        Seq.allPairs A B
        |> Seq.map (fun (a:'a, b:'b) -> if predicate a b then (a, Some(b)) else (a, None))
        |> Seq.distinct

    let rightOuterJoin (A:'a seq) (B:'b seq) (predicate:'a ->'b -> bool):('a option * 'b) seq =
        Seq.allPairs A B
        |> Seq.map (fun (a:'a, b:'b) -> if predicate a b then (Some(a), b) else (None, b))
        |> Seq.distinct

    let fullOuterJoin (A:'a seq) (B:'b seq) (predicate:'a ->'b -> bool):('a option * 'b option) seq =
        let left:('a option * 'b option) seq =
            (leftOuterJoin A B predicate)
            |> Seq.map (fun (a:'a, b:'b option) -> (Some(a), b))
        let right:('a option * 'b option) seq =
            (rightOuterJoin A B predicate)
            |> Seq.map (fun (a:'a option, b:'b) -> (a, Some(b)))
        Seq.append left right
        |> Seq.distinct
