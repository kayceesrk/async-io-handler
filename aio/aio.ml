(* Asynchronous IO scheduler.
 *
 * For each blocking action, if the action can be performed immediately, then it
 * is. Otherwise, the thread performing the blocking task is suspended and
 * automatically wakes up when the action completes. The suspend/resume is
 * transparent to the programmer.
 *)

type file_descr = Unix.file_descr
type sockaddr = Unix.sockaddr
type msg_flag = Unix.msg_flag

effect Fork  : (unit -> unit) -> unit
effect Yield : unit
effect Accept : file_descr -> (file_descr * sockaddr)
effect Recv : file_descr * bytes * int * int * msg_flag list -> int
effect Send : file_descr * bytes * int * int * msg_flag list -> int
effect Sleep : float -> unit

let fork f =
  perform (Fork f)

let yield () =
  perform Yield

let accept fd =
  perform (Accept fd)

let recv fd buf pos len mode =
  perform (Recv (fd, buf, pos, len, mode))

let send fd bus pos len mode =
  perform (Send (fd, bus, pos, len, mode))

let sleep timeout =
  perform (Sleep timeout)

external poll_rd : Unix.file_descr -> bool = "lwt_unix_readable"
external poll_wr : Unix.file_descr -> bool = "lwt_unix_writable"

type state =
  { run_q    : (unit -> unit) Queue.t;
    read_ht  : (file_descr, Lwt_engine.event * (unit -> unit) Lwt_sequence.t) Hashtbl.t;
    write_ht : (file_descr, Lwt_engine.event * (unit -> unit) Lwt_sequence.t) Hashtbl.t; }

let init () =
  { run_q = Queue.create ();
    read_ht = Hashtbl.create 13;
    write_ht = Hashtbl.create 13 }

let register_readable fd seq = 
    Lwt_engine.on_readable fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) seq)

let register_writable fd seq = 
    Lwt_engine.on_writable fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) seq)

let dummy = Lwt_sequence.add_r ignore (Lwt_sequence.create ())

let block_accept st fd k =
  let node = ref dummy in
  let seq =
    try snd @@ Hashtbl.find st.read_ht fd
    with Not_found -> 
      let seq = Lwt_sequence.create () in
      let ev = register_readable fd seq in
      Hashtbl.add st.read_ht fd (ev, seq);
      seq
  in
  node := Lwt_sequence.add_r (fun () -> 
    Lwt_sequence.remove !node; 
    (* Fixme: Might block. See lwt_unix.ml for non-blocking implementation. *)
    let res = Unix.accept fd in
    Queue.push (fun () -> continue k res) st.run_q) seq

let block_recv st fd buf pos len mode k =
  let node = ref dummy in
  let seq =
    try snd @@ Hashtbl.find st.read_ht fd
    with Not_found -> 
      let seq = Lwt_sequence.create () in
      let ev = register_readable fd seq in
      Hashtbl.add st.read_ht fd (ev, seq);
      seq
  in
  node := Lwt_sequence.add_r (fun () -> 
    Lwt_sequence.remove !node; 
    (* Fixme: Might block. See lwt_unix.ml for non-blocking implementation. *)
    let res = Unix.recv fd buf pos len mode in
    Queue.push (fun () -> continue k res) st.run_q) seq

let block_send st fd buf pos len mode k =
  let node = ref dummy in
  let seq =
    try snd @@ Hashtbl.find st.write_ht fd
    with Not_found -> 
      let seq = Lwt_sequence.create () in
      let ev = register_writable fd seq in
      Hashtbl.add st.write_ht fd (ev, seq);
      seq
  in
  node := Lwt_sequence.add_r (fun () -> 
    Lwt_sequence.remove !node; 
    (* Fixme: Might block. See lwt_unix.ml for non-blocking implementation. *)
    let res = Unix.send fd buf pos len mode in
    Queue.push (fun () -> continue k res) st.run_q) seq

let block_sleep st delay k =
  ignore @@ Lwt_engine.on_timer delay false (fun ev -> 
    Lwt_engine.stop_event ev; 
    Queue.push (continue k) st.run_q)

let clean st =
  let clean_ht ht = 
    let fd_list = 
      (* XXX: Use Hashtbl.filter_map_inplace *)
      Hashtbl.fold (fun fd (ev,seq) acc -> 
        if Lwt_sequence.is_empty seq then
          (Lwt_engine.stop_event ev; fd::acc)
        else acc) ht []
    in
    List.iter (fun fd -> Hashtbl.remove ht fd) fd_list
  in
  clean_ht st.read_ht;
  clean_ht st.write_ht

let rec schedule st =
  if Queue.is_empty st.run_q then (* No runnable threads *)
    if Hashtbl.length st.read_ht = 0 &&
       Hashtbl.length st.write_ht = 0 &&
       Lwt_engine.timer_count () = 0 then () (* We are done *)
    else perform_io st
  else (* Still have runnable threads *)
    Queue.pop st.run_q ()

and perform_io st =
  Lwt_engine.iter true;
  (* TODO: Cleanup should be performed laziyly for performance. *)
  clean st;
  schedule st

let run main =
  let st = init () in
  let rec fork st f =
    match f () with
    | () -> schedule st
    | exception exn ->
        print_string (Printexc.to_string exn);
        schedule st
    | effect Yield  k ->
        Queue.push (continue k) st.run_q;
        schedule st
    | effect (Fork f) k ->
        Queue.push (continue k) st.run_q;
        fork st f
    | effect (Accept fd) k ->
        if poll_rd fd then begin
          let res = Unix.accept fd in
          continue k res
        end else begin
          block_accept st fd k;
          schedule st
        end
    | effect (Recv (fd, buf, pos, len, mode)) k ->
        if poll_rd fd then begin
          let res = Unix.recv fd buf pos len mode in
          continue k res
        end else begin
          block_recv st fd buf pos len mode k;
          schedule st
        end
    | effect (Send (fd, buf, pos, len, mode)) k ->
        if poll_wr fd then begin
          let res = Unix.send fd buf pos len mode in
          continue k res
        end else begin
          block_send st fd buf pos len mode k;
          schedule st
        end
    | effect (Sleep t) k ->
        if t <= 0. then continue k ()
        else begin
          block_sleep st t k;
          schedule st
        end
  in
  fork st main
