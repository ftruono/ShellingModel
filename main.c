#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdbool.h>
#include <time.h>
#include <math.h>
#include <memory.h>
#include "mpi.h"


enum STATUS {
    RED, BLUE, EMPTY
};


enum ALLOCATION {
    ALLOCATED,
    NOT_ALLOCATED,
    INVALID
};

typedef struct {
    int row;
    int col;
    int satisfation;
    int red;
    int blue;
} InitializeMsg;


typedef struct {
    enum STATUS status;
    bool locked;
    float satisfacion;
} City;

typedef struct {
    int original_proc;
    int destination_proc;
    int x;
    int y;
    int last_edit_by;
    enum STATUS content;
    enum ALLOCATION allocation_result;
} UnHappy;

MPI_Datatype make_type_for_initialize_msg();

MPI_Datatype make_type_for_city();

MPI_Datatype make_type_for_unhappy();

void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion);

void handle_input(char *arg, int *store, bool isGrid);

char decode_enum(enum STATUS status);

//int *reduceProcesses(int processes, int grid_size);

void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type);

InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red);

City **initialize_cache(int col);

City **initialize_grid_city(InitializeMsg initialize_struct);

UnHappy *initialize_un_happy(int size);

void default_un_happy_values(UnHappy *list, int start, int size);

void push_random_values(City **grid_city, int max_row, int col, int content_legth, enum STATUS status);

void print_grid(City **grid_city, int row, int col);

//int calculate_offset(int proc, int x, int y, int col, int row);

int get_process_from_offset(int offset, int row, int col);

void
do_request(int proc, City **grid_city, City **cache, int row, int col, int last_processor, MPI_Datatype mpi_city_type);

int
check_nearest(int proc, City **grid_city, City **cache, int row, int col, int satisfaction, UnHappy *unhappy_list,
              int last_process);

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf);

void check_satisfaction_vertical_on_cache(City **grid_city, City **cache, int i, int j, int pos, int *count_near,
                                          int *satisf);

//enum RANGE is_in_my_range(int value, int min_range, int max_range, int max_size);

void check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf);

void
check_satisfaction_oblique_on_cache(City **grid_city, City **cache, int i, int j, int pos_x, int pos_y, int *count_near,
                                    int *satisf);

void update_if_empty(int *count_near, int *satisf);


//TODO da eliminare;
void print_cache(City **cache, int col, int rank, int max_rank);

void mock_data2x6(City **grid_city);

void mock_data4x4(City **grid_city);

void mock_data2x4(City **grid_city);


void resize_unhappy(UnHappy **list, int tot_unsadisfied, int local_unsadisfied, int rank);

void send_broadcast_unhappy(UnHappy *list, int rank, int processes, int size, MPI_Datatype mpi_unhappy);


UnHappy *
receive_broadcast_unhappy(UnHappy *list, int unsatisfied, int rank, int processes,
                          MPI_Datatype mpi_unhappy);

void
receive_broadcast_unhappy_in_place(UnHappy *total_proc, int tot_unsatisfied, int local_unsatisfied, int rank, int processes,
                                   MPI_Datatype mpi_unhappy);

UnHappy *gather_unhappy(UnHappy *list_to_send, int unsatisfied, int processes,
                        MPI_Datatype mpi_unhappy);

void search_first_empty(City **grid_city, int row, int col, int *x, int *y);

void try_to_move(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes, int row, int col);

int update_with_empty_space(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes);

void syncronized_unhappy(UnHappy *total_proc, int unsatisfied, int processes, MPI_Datatype mpi_unhappy);

void commit_difference(UnHappy *total_proc, UnHappy *temp, int rank, int tot_unsadisfied, int local_unsadisfied);

void clear_memory(City **grid, City **cache, int row);


/*
 TESTING
int testP0_2x6() {
    City **grid, **cache;
    UnHappy *unhappy_list;
    cache = initialize_cache(6);

    grid = (City **) malloc(2 * sizeof(City *));
    for (int i = 0; i < 2; ++i) {
        grid[i] = (City *) malloc(6 * sizeof(City));
    }
    mock_data2x6(grid);


    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    cache[1][0] = red;
    cache[1][1] = red;
    cache[1][2] = empty;
    cache[1][3] = empty;
    cache[1][4] = blue;
    cache[1][5] = empty;

    check_nearest(0, grid, cache, 2, 6,
                  50, unhappy_list, 3);
}


int testP1_2x6() {
    City **grid, **cache;
    UnHappy *unhappy_list;
    cache = initialize_cache(6);

    grid = (City **) malloc(2 * sizeof(City *));
    for (int i = 0; i < 2; ++i) {
        grid[i] = (City *) malloc(6 * sizeof(City));
    }
    mock_data2x6(grid);


    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    cache[0][0] = red;
    cache[0][1] = red;
    cache[0][2] = empty;
    cache[0][3] = empty;
    cache[0][4] = blue;
    cache[0][5] = empty;

    cache[1][0] = red;
    cache[1][1] = red;
    cache[1][2] = empty;
    cache[1][3] = blue;
    cache[1][4] = red;
    cache[1][5] = red;

    check_nearest(1, grid, cache, 2, 6,
                  50, unhappy_list, 3);
}

int testP2_2x6() {
    City **grid, **cache;
    UnHappy *unhappy_list;
    cache = initialize_cache(6);

    grid = (City **) malloc(2 * sizeof(City *));
    for (int i = 0; i < 2; ++i) {
        grid[i] = (City *) malloc(6 * sizeof(City));
    }
    mock_data2x6(grid);


    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    cache[0][0] = red;
    cache[0][1] = red;
    cache[0][2] = empty;
    cache[0][3] = empty;
    cache[0][4] = blue;
    cache[0][5] = empty;

    check_nearest(2, grid, cache, 2, 6,
                  50, unhappy_list, 3);
}

 */


int test_resize() {
    UnHappy *test = initialize_un_happy(5);
    resize_unhappy(&test, 15, 5, 0);
    return 0;
}

//TODO delete
char decode_allocation(enum ALLOCATION allocation) {
    switch (allocation) {
        case NOT_ALLOCATED:
            return 'N';
        case ALLOCATED:
            return 'A';
        case INVALID:
            return 'X';
    }
}

int main(int argc, char *argv[]) {

    int processes, rank;
    int local_unsatisfied = 0;
    int tot_unsadisfied = 0;
    InitializeMsg startup_info;
    City **grid, **cache;
    UnHappy *unhappy_list;
    MPI_Status status_in_msg;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    srand(time(NULL) + rank);
    MPI_Datatype mpi_initialize_message_type = make_type_for_initialize_msg();
    MPI_Datatype mpi_city_type = make_type_for_city();
    MPI_Datatype mpi_unhappy = make_type_for_unhappy();
    if (rank == 0) {
        int grid_size, red_pop, blue_pop, satisfaction, empty;

        if (argc < 5) {
            get_input_from_terminal(&grid_size, &red_pop, &blue_pop, &empty, &satisfaction);
        } else {
            handle_input(argv[1], &grid_size, true);
            handle_input(argv[2], &blue_pop, false);
            red_pop = 100 - blue_pop;
            handle_input(argv[4], &empty, false);
            handle_input(argv[5], &satisfaction, false);
        }
        int total_cells = grid_size * grid_size;
        int total_empty = (total_cells * empty) / 100;
        total_cells -= total_empty;
        int total_blue = (total_cells * blue_pop) / 100;
        int total_red = (total_cells * red_pop) / 100;
        /*printf("celle totali %d \n vuoti %d \n totali-vuoti %d \n blu totali: %d \n rossi totali %d \n",
               grid_size * grid_size, total_empty, total_cells, total_blue, total_red);*/
        //TODO assunzione che processes < grid_size (P<N) - assunto che la perdita di qualche cella non è un problema
        int splitted_blue = total_blue / processes;
        int splitted_red = total_red / processes;
        int splitted_row = grid_size / processes;

        //printf("row %d \n blue x proc %d \n red x proc %d \n", splitted_row, splitted_blue, splitted_red);
        int excluded = grid_size % processes;
        if (excluded != 0) {
            int assigned_processes = rand() % processes;
            int temp_proc_row = splitted_row + excluded;

            startup_info = create_initialize_message(assigned_processes == 0 ? temp_proc_row : splitted_row, grid_size,
                                                     satisfaction, splitted_blue,
                                                     splitted_red);
            for (int i = 1; i < processes; ++i) {
                send_startup_information(
                        create_initialize_message(i == assigned_processes ? temp_proc_row : splitted_row, grid_size,
                                                  satisfaction, splitted_blue,
                                                  splitted_red),
                        i,
                        mpi_initialize_message_type
                );
            }


        } else { //nessun resto nella divisione
            startup_info = create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue,
                                                     splitted_red); // for 0° process

            for (int i = 1; i < processes; ++i) {
                send_startup_information(
                        create_initialize_message(splitted_row, grid_size, satisfaction, splitted_blue, splitted_red),
                        i,
                        mpi_initialize_message_type
                );
            }
        }

    } else {
        MPI_Recv(&startup_info, 1, mpi_initialize_message_type, 0, 0, MPI_COMM_WORLD, &status_in_msg);
    }

    grid = initialize_grid_city(startup_info);
    cache = initialize_cache(startup_info.col);
    //printf("rank %d - my grid %d %d \n", rank, startup_info.row, startup_info.col);
    //print_grid(grid, startup_info.row, startup_info.col);
    MPI_Barrier(MPI_COMM_WORLD);
    do {
        //TODO BUG: memory leak nella creazione di unhappy list!
        unhappy_list = initialize_un_happy(startup_info.col * startup_info.row);
        do_request(rank, grid, cache, startup_info.row, startup_info.col, processes, mpi_city_type);

        local_unsatisfied = check_nearest(rank, grid, cache, startup_info.row, startup_info.col,
                                          startup_info.satisfation, unhappy_list, processes);
        MPI_Allreduce(&local_unsatisfied, &tot_unsadisfied, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        printf("[RANK %d] - non soddisfatti locali: %d - non soddisfatti totali %d \n", rank, local_unsatisfied,
               tot_unsadisfied);
        resize_unhappy(&unhappy_list, tot_unsadisfied, local_unsatisfied, rank);


        UnHappy *unHappy_all_proc = gather_unhappy(unhappy_list, tot_unsadisfied, processes, mpi_unhappy);



        /*printf("start cycle \n");*/
        //TODO riportare a come era

        do {
            //printf("rank %d ---print \n", rank);
            //print_grid(grid, startup_info.row, startup_info.col);
            try_to_move(unHappy_all_proc, grid, rank, tot_unsadisfied, processes, startup_info.row, startup_info.col);
            MPI_Barrier(MPI_COMM_WORLD);
            send_broadcast_unhappy(unHappy_all_proc, rank, processes, tot_unsadisfied * processes, mpi_unhappy);
            receive_broadcast_unhappy_in_place(unHappy_all_proc, tot_unsadisfied,local_unsatisfied, rank, processes, mpi_unhappy);

            break;
        } while (update_with_empty_space(unHappy_all_proc, grid, rank, tot_unsadisfied, processes) > 0);

        MPI_Barrier(MPI_COMM_WORLD);
        free(unhappy_list);
        free(unHappy_all_proc);
        tot_unsadisfied=0;
    } while (tot_unsadisfied > 0);

    printf("finito \n");
    printf("[RANK %d] - my grid %d %d \n", rank, startup_info.row, startup_info.col);
    print_grid(grid, startup_info.row, startup_info.col);

    clear_memory(grid, cache, startup_info.row);
    MPI_Type_free(&mpi_city_type);
    MPI_Type_free(&mpi_initialize_message_type);
    MPI_Type_free(&mpi_unhappy);
    MPI_Finalize();
    return 0;
}


MPI_Datatype make_type_for_initialize_msg() {
    MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_message_type;
    MPI_Aint offsets[5];
    int blocklengths[5] = {1, 1, 1, 1, 1};
    offsets[0] = offsetof(InitializeMsg, row);
    offsets[1] = offsetof(InitializeMsg, col);
    offsets[2] = offsetof(InitializeMsg, satisfation);
    offsets[3] = offsetof(InitializeMsg, red);
    offsets[4] = offsetof(InitializeMsg, blue);

    MPI_Type_create_struct(5, blocklengths, offsets, types, &mpi_message_type);
    MPI_Type_commit(&mpi_message_type);

    return mpi_message_type;
}


MPI_Datatype make_type_for_city() {
    MPI_Datatype types[3] = {MPI_INT, MPI_C_BOOL, MPI_FLOAT};
    MPI_Datatype mpi_city;
    MPI_Aint offsets[3];
    int blocklengths[3] = {1, 1, 1};
    offsets[0] = offsetof(City, status);
    offsets[1] = offsetof(City, locked);
    offsets[2] = offsetof(City, satisfacion);

    MPI_Type_create_struct(3, blocklengths, offsets, types, &mpi_city);
    MPI_Type_commit(&mpi_city);

    return mpi_city;
}

MPI_Datatype make_type_for_unhappy() {
    MPI_Datatype types[7] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_unhappy;
    MPI_Aint offsets[7];
    int block_lengths[7] = {1, 1, 1, 1, 1, 1, 1};
    offsets[0] = offsetof(UnHappy, original_proc);
    offsets[1] = offsetof(UnHappy, destination_proc);
    offsets[2] = offsetof(UnHappy, x);
    offsets[3] = offsetof(UnHappy, y);
    offsets[4] = offsetof(UnHappy, last_edit_by);
    offsets[5] = offsetof(UnHappy, content);
    offsets[6] = offsetof(UnHappy, allocation_result);

    MPI_Type_create_struct(7, block_lengths, offsets, types, &mpi_unhappy);
    MPI_Type_commit(&mpi_unhappy);

    return mpi_unhappy;
}


void get_input_from_terminal(int *grid_size, int *red_pop, int *blue_pop, int *empty, int *satisfatcion) {
    char grid_string[10], blue_string[3], empty_string[3], satisfaction_string[3];
    printf("Size of grid:\n");
    scanf("%s", grid_string);
    handle_input(grid_string, grid_size, true);

    printf("blue(B) population in %% \n");
    scanf("%s", blue_string);
    handle_input(blue_string, blue_pop, false);
    *red_pop = 100 - *blue_pop;

    printf("Empty space in %% \n");
    scanf("%s", empty_string);
    handle_input(empty_string, empty, false);

    printf("Satisfatcion agent in %% \n");
    scanf("%s", satisfaction_string);
    handle_input(satisfaction_string, satisfatcion, false);
}


void handle_input(char *arg, int *store, bool isGrid) {
    char *p;
    int conv = strtol(arg, &p, 10);
    if (*p != '\0' || (isGrid && conv <= 1) || (!isGrid && (conv < 0 || conv > 100))) {
        printf("There is an error on conversion or invalid range");
        exit(-1);
    } else {
        *store = conv;
    }
}


void print_cache(City **cache, int col, int rank, int max_rank) {
    if (rank == 0) return;
    if (rank == max_rank - 1) return;
    printf("rank : %d e cache \n", rank);
    for (int i = 0; i < 2; ++i) {

        for (int j = 0; j < col; ++j) {
            printf("%c", decode_enum(cache[i][j].status));
        }
        printf("\n");
    }
}

/*
int *reduceProcesses(int processes, int grid_size) {
    while (processes >= grid_size) {
        processes /= 2;
    }
    int *valid = (int *) malloc(sizeof(int) * processes);
    for (int i = 0; i < processes; ++i) {
        valid[i] = i;
    }
    return valid;
}
*/

InitializeMsg create_initialize_message(int row, int col, int satisfaction, int blue, int red) {
    InitializeMsg msg;
    msg.col = col;
    msg.satisfation = satisfaction;
    msg.blue = blue;
    msg.red = red;
    msg.row = row;
    return msg;
}

void send_startup_information(InitializeMsg msg, int proc,
                              MPI_Datatype mpi_initialize_message_type) {

    //printf("Messaggio di inizializzazione red: %d blue:%d  \n", msg.red, msg.blue);
    MPI_Send(&msg, 1, mpi_initialize_message_type, proc, 0, MPI_COMM_WORLD);
}


City **initialize_grid_city(InitializeMsg initialize_struct) {
    //printf("Push random: ");
    City **grid_city = (City **) malloc(initialize_struct.row * sizeof(City *));
    for (int i = 0; i < initialize_struct.row; ++i) {
        grid_city[i] = (City *) malloc(initialize_struct.col * sizeof(City));
    }
    for (int i = 0; i < initialize_struct.row; ++i) {
        for (int j = 0; j < initialize_struct.col; ++j) {
            grid_city[i][j].status = EMPTY;
            grid_city[i][j].locked = false;
            grid_city[i][j].satisfacion = 0;
        }
    }
    mock_data2x4(grid_city);
    //mock_data2x6(grid_city);
    //mock_data4x4(grid_city);
    //push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.red, RED);
    //push_random_values(grid_city, initialize_struct.row, initialize_struct.col, initialize_struct.blue, BLUE);

    return grid_city;
}

City **initialize_cache(int col) {
    City **cache = (City **) malloc(2 * sizeof(City *));
    for (int i = 0; i < 2; ++i) {
        cache[i] = (City *) malloc(col * sizeof(City));
    }
    return cache;
}

UnHappy *initialize_un_happy(int size) {
    UnHappy *list = (UnHappy *) malloc(size * sizeof(UnHappy));
    default_un_happy_values(list, 0, size);
    return list;
}

void default_un_happy_values(UnHappy *list, int start, int size) {
    for (int i = start; i < size; ++i) {
        list[i].original_proc = -1;
        list[i].destination_proc = -1;
        list[i].allocation_result = INVALID;
        list[i].content = EMPTY;
        list[i].x = -1;
        list[i].y = -1;
        list[i].last_edit_by = -1;
    }
}

void mock_data2x6(City **grid_city) {

    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    grid_city[0][0] = red;
    grid_city[0][1] = red;
    grid_city[0][2] = empty;
    grid_city[0][3] = empty;
    grid_city[0][4] = blue;
    grid_city[0][5] = red;

    grid_city[1][0] = blue;
    grid_city[1][1] = blue;
    grid_city[1][2] = blue;
    grid_city[1][3] = red;
    grid_city[1][4] = red;
    grid_city[1][5] = red;
}

void mock_data2x4(City **grid_city) {

    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    grid_city[0][0] = red;
    grid_city[0][1] = empty;
    grid_city[0][2] = empty;
    grid_city[0][3] = blue;

    grid_city[1][0] = blue;
    grid_city[1][1] = blue;
    grid_city[1][2] = red;
    grid_city[1][3] = empty;
}

void mock_data4x4(City **grid_city) {
    City red;
    red.satisfacion = 30;
    red.locked = true;
    red.status = RED;
    City blue;
    blue.satisfacion = 40;
    blue.locked = true;
    blue.status = BLUE;
    City empty;
    empty.satisfacion = 0;
    empty.locked = false;
    empty.status = EMPTY;

    grid_city[0][0] = red;
    grid_city[0][1] = empty;
    grid_city[0][2] = empty;
    grid_city[0][3] = blue;

    grid_city[1][0] = blue;
    grid_city[1][1] = blue;
    grid_city[1][2] = red;
    grid_city[1][3] = empty;

    grid_city[2][0] = blue;
    grid_city[2][1] = red;
    grid_city[2][2] = red;
    grid_city[2][3] = blue;

    grid_city[3][0] = empty;
    grid_city[3][1] = red;
    grid_city[3][2] = empty;
    grid_city[3][3] = empty;
}

void push_random_values(City **grid_city, int max_row, int max_col, int content_legth, enum STATUS status) {
    int next = content_legth;
    while (next > 0) {
        int row = rand() % max_row;
        int col = rand() % max_col;
        if (!grid_city[row][col].locked) {
            grid_city[row][col].status = status;
            grid_city[row][col].locked = (status != EMPTY);
            grid_city[row][col].satisfacion = 0;
            --next;
        }

    }
    //printf("Inizializzato\n");

}


void
do_request(int proc, City **grid_city, City **cache, int row, int col, int last_processor,
           MPI_Datatype mpi_city_type) { //cache[0] parte alta cache[1] parte bassa
    MPI_Status status1, status2;
    if (last_processor > 1) {
        if (proc == 0) {
            MPI_Send(grid_city[row - 1], col, mpi_city_type, proc + 1, 2, MPI_COMM_WORLD);
            MPI_Recv(cache[1], col, mpi_city_type, proc + 1, 1, MPI_COMM_WORLD, &status1);
            //MPI_Wait(&req1, MPI_STATUS_IGNORE);

            /*
            printf("cache proc 0 \n");
            for (int i = 0; i < col; ++i) {
                printf("%c ", decode_enum(cache[1][i].status));
            }
            printf("\n");
             */
        } else if (proc == last_processor - 1) {
            MPI_Send(grid_city[0], col, mpi_city_type, proc - 1, 1, MPI_COMM_WORLD);
            MPI_Recv(cache[0], col, mpi_city_type, proc - 1, 2, MPI_COMM_WORLD, &status1);
            /*for (int i = 0; i < col; ++i) {
                printf("%d %2f %c", cache[0][i].locked, cache[0][i].satisfacion, decode_enum(cache[0][i].status));
            }
            printf("\n");
             */
        } else {
            MPI_Send(grid_city[0], col, mpi_city_type, proc - 1, 1, MPI_COMM_WORLD);
            MPI_Send(grid_city[row - 1], col, mpi_city_type, proc + 1, 2, MPI_COMM_WORLD);
            //ricezione
            MPI_Recv(cache[0], col, mpi_city_type, proc - 1, 2, MPI_COMM_WORLD, &status1);
            MPI_Recv(cache[1], col, mpi_city_type, proc + 1, 1, MPI_COMM_WORLD, &status2);
            /*printf("cache di %d \n", proc);
            for (int i = 0; i < col; ++i) {
                printf("%c", decode_enum(cache[0][i].status));
            }
            printf("\n");
            for (int i = 0; i < col; ++i) {
                printf("%c", decode_enum(cache[1][i].status));
            }*/


        }
    }


}

int
check_nearest(int proc, City **grid_city, City **cache, int row, int col, int satisfaction, UnHappy *unhappy_list,
              int last_process) {
    int size = col * row;
    int unsatisfaied = 0;
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            if (grid_city[i][j].status != EMPTY) {
                int satisf = 0;
                int count_near = 0;
                int left = j - 1;
                if (left >= 0) {
                    check_satisfaction_horizontal(grid_city, i, j, left, &count_near, &satisf);
                }
                int rigth = j + 1;
                if (rigth < col) {
                    check_satisfaction_horizontal(grid_city, i, j, rigth, &count_near, &satisf);
                }

                int top = i - 1;
                if (top >= 0) {
                    check_satisfaction_vertical(grid_city, i, j, top, &count_near, &satisf);
                } else {
                    if (proc != 0) {
                        check_satisfaction_vertical_on_cache(grid_city, cache, i, j, 0, &count_near, &satisf);
                    }
                }


                int bottom = i + 1;
                if (bottom < row) {
                    check_satisfaction_vertical(grid_city, i, j, bottom, &count_near, &satisf);
                } else {
                    if (proc != last_process - 1) {
                        check_satisfaction_vertical_on_cache(grid_city, cache, i, j, 1, &count_near, &satisf);
                    }
                }


                int no_x = i - 1;
                int no_y = j - 1;
                if (no_x >= 0 && no_y >= 0) {
                    check_satisfaction_oblique(grid_city, i, j, no_x, no_y, &count_near, &satisf);
                } else {
                    if (no_y >= 0 && proc != 0) {
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 0, no_y, &count_near, &satisf);
                    }
                }

                int ne_x = i - 1;
                int ne_y = j + 1;
                if (ne_x >= 0 && ne_y < col) {
                    check_satisfaction_oblique(grid_city, i, j, ne_x, ne_y, &count_near, &satisf);
                } else {
                    if (ne_y < col && proc != 0)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 0, ne_y, &count_near, &satisf);
                }


                int so_x = i + 1;
                int so_y = j - 1;
                if (so_x < row && so_y >= 0) {
                    check_satisfaction_oblique(grid_city, i, j, so_x, so_y, &count_near, &satisf);
                } else {
                    if (so_y >= 0 && proc != last_process - 1)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 1, so_y, &count_near, &satisf);
                }


                int se_x = i + 1;
                int se_y = j + 1;
                if (se_x < row && se_y < col) {
                    check_satisfaction_oblique(grid_city, i, j, se_x, se_y, &count_near, &satisf);
                } else {
                    if (se_y < col && proc != last_process - 1)
                        check_satisfaction_oblique_on_cache(grid_city, cache, i, j, 1, se_y, &count_near, &satisf);
                }

                float sats = (float) satisf / count_near;
                grid_city[i][j].satisfacion = sats * 100;
                /*printf("%d %d %d : %f  count: %d e satisf %d \n", proc, i, j, grid_city[i][j].satisfacion,
                       count_near,
                       satisf);*/
                if ((int) grid_city[i][j].satisfacion < satisfaction) {
                    UnHappy unHappy;
                    unHappy.content = grid_city[i][j].status;
                    unHappy.x = i;
                    unHappy.y = j;
                    unHappy.last_edit_by = -1;
                    unHappy.allocation_result = NOT_ALLOCATED;
                    unHappy.original_proc = proc;
                    unHappy.destination_proc = rand() % last_process;
                    unhappy_list[unsatisfaied++] = unHappy;
                    //printf("insodisfatto %d (%d-%d) sat: %2.f \n", unsatisfaied, i, j, grid_city[i][j].satisfacion);
                }
            }
        }
    }
    return unsatisfaied;
}

void check_satisfaction_horizontal(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[i][pos].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[i][pos].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void check_satisfaction_vertical(City **grid_city, int i, int j, int pos, int *count_near, int *satisf) {
    if (grid_city[pos][j].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos][j].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void check_satisfaction_vertical_on_cache(City **grid_city, City **cache, int i, int j, int pos, int *count_near,
                                          int *satisf) {
    if (cache[pos][j].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == cache[pos][j].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void
check_satisfaction_oblique(City **grid_city, int i, int j, int pos_x, int pos_y, int *count_near, int *satisf) {
    if (grid_city[pos_x][pos_y].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == grid_city[pos_x][pos_y].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void
check_satisfaction_oblique_on_cache(City **grid_city, City **cache, int i, int j, int pos_x, int pos_y,
                                    int *count_near,
                                    int *satisf) {
    if (cache[pos_x][pos_y].locked) {
        (*count_near)++;
        if (grid_city[i][j].status == cache[pos_x][pos_y].status) {
            (*satisf)++;
        }
    } else {
        update_if_empty(count_near, satisf);
    }
}

void update_if_empty(int *count_near, int *satisf) {
    (*count_near)++;
    (*satisf)++;
}

/*
enum RANGE is_in_my_range(int value, int min_range, int max_range, int max_size) {
    if (value >= min_range && value <= max_range)
        return IN_MY_RANGE;
    else if (value < 0 || value > max_size) {
        return INVALID;
    } else {
        return OUT_OF_RANGE;
    }
}


int calculate_offset(int proc, int x, int y, int col, int row) {
    return (proc * col * row) + (x * col + y);
}
 */

int get_process_from_offset(int offset, int row, int col) {
    int proc = -1;
    row -= 1;
    col -= 1;
    for (; offset > 0; ++proc) {
        offset -= row * col;
    }
    return proc;
}


void print_grid(City **grid_city, int row, int col) {
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            printf("%c  ", decode_enum(grid_city[i][j].status));
        }
        printf("\n");
    }
}

char decode_enum(enum STATUS status) {
    switch (status) {
        case BLUE:
            return 'B';
        case RED:
            return 'R';
        case EMPTY:
            return 'E';
        default:
            return 'X';
    }
}

void resize_unhappy(UnHappy **list, int tot_unsadisfied, int local_unsadisfied, int rank) {

    *list = realloc(*list, tot_unsadisfied * sizeof(UnHappy));
    default_un_happy_values(*list, local_unsadisfied, tot_unsadisfied);
}

void send_broadcast_unhappy(UnHappy *list, int rank, int processes, int size, MPI_Datatype mpi_unhappy) {
    for (int i = 0; i < processes; ++i) {
        if (rank != i) {
            MPI_Send(list, size, mpi_unhappy, i, 0, MPI_COMM_WORLD);
            printf("[RANK %d] sended to %d\n", rank, i);
        }
    }
    //
}

UnHappy *
receive_broadcast_unhappy(UnHappy *list, int unsatisfied, int rank, int processes,
                          MPI_Datatype mpi_unhappy) {
    //printf("i'm receiving \n");
    int size = unsatisfied * processes;
    UnHappy *temp = (UnHappy *) malloc(sizeof(UnHappy) * unsatisfied);
    UnHappy *unHappy_total_proc = (UnHappy *) malloc(sizeof(UnHappy) * size);
    MPI_Status status;
    for (int i = 0; i < processes; ++i) {
        MPI_Recv(temp, unsatisfied, mpi_unhappy, i, 0, MPI_COMM_WORLD, &status);
        memcpy(unHappy_total_proc + (i * unsatisfied), temp, sizeof(UnHappy) * unsatisfied);
    }
    /*
    for (int j = 0; j < size; ++j) {
        printf("unHappy: rank %d pos %d : orig:%d dest:%d all:%c x:%d y:%d leb:%d \n", rank, j,
               unHappy_total_proc[j].original_proc,
               unHappy_total_proc[j].destination_proc,
               decode_allocation(unHappy_total_proc[j].allocation_result), unHappy_total_proc[j].x,
               unHappy_total_proc[j].y, unHappy_total_proc[j].last_edit_by);
    }*/
    free(temp);
    return unHappy_total_proc;
}

void
receive_broadcast_unhappy_in_place(UnHappy *total_proc, int tot_unsatisfied, int local_unsatisfied, int rank, int processes,
                                   MPI_Datatype mpi_unhappy) {
    //printf("ricezione \n");
    int size = tot_unsatisfied * processes;
    UnHappy *temp = (UnHappy *) malloc(sizeof(UnHappy) * size);
    MPI_Status status;
    for (int i = 0; i < processes; ++i) {
        if (rank != i) {
            MPI_Recv(temp, size, mpi_unhappy, i, 0, MPI_COMM_WORLD, &status);
            printf("[RANK %d] received from %d\n", rank, i);

            commit_difference(total_proc, temp, i, tot_unsatisfied, local_unsatisfied);
        }
    }

    for (int j = 0; j < size; ++j) {
        if (total_proc[j].allocation_result != INVALID) {
            printf("[RANK %d] totalProc-> pos:%d - orig:%d dest:%d all:%c x:%d y:%d leb:%d \n", rank, j,
                   total_proc[j].original_proc,
                   total_proc[j].destination_proc,
                   decode_allocation(total_proc[j].allocation_result), total_proc[j].x,
                   total_proc[j].y, total_proc[j].last_edit_by);
        }
    }
    free(temp);
}

UnHappy *gather_unhappy(UnHappy *list_to_send, int unsatisfied, int processes,
                        MPI_Datatype mpi_unhappy) {
    int size = unsatisfied * processes;
    UnHappy *unHappy_total_proc = (UnHappy *) malloc(sizeof(UnHappy) * size);

    MPI_Allgather(list_to_send, unsatisfied, mpi_unhappy, unHappy_total_proc, unsatisfied, mpi_unhappy, MPI_COMM_WORLD);
    return unHappy_total_proc;
}


void commit_difference(UnHappy *total_proc, UnHappy *temp, int rank, int tot_unsadisfied, int local_unsadisfied) {
    int start = rank * tot_unsadisfied;
    int stop = start + local_unsadisfied;
    printf("[RANK %d] - local_unsad: %d start: %d stop:%d \n",rank,local_unsadisfied,start,stop);
    for (int i = start; i < stop; ++i) {
        total_proc[i].allocation_result = temp[i].allocation_result;
        total_proc[i].destination_proc = temp[i].destination_proc;
        total_proc[i].last_edit_by = -1;

    }
}

void
try_to_move(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes, int row, int col) {
    int size = unsatisfied * processes;
    for (int i = 0; i < size; ++i) {
        //quelli invalidi è padding
        if (unhappy_list[i].destination_proc == rank && unhappy_list[i].allocation_result == NOT_ALLOCATED) {
            int nx, ny;
            search_first_empty(grid_city, row, col, &nx, &ny); // ha senso usare la prima cella vuota del processo?
            if (nx != -1 && ny != -1) {
                grid_city[nx][ny].status = unhappy_list[i].content;
                grid_city[nx][ny].locked = true;
                grid_city[nx][ny].satisfacion = 0;
                /*printf("[RANK %d] è stato spostato (%d,%d) di %d -> (%d,%d) di %d \n", rank, unhappy_list[i].x,
                       unhappy_list[i].y,
                       unhappy_list[i].original_proc, nx, ny, rank);*/
                unhappy_list[i].allocation_result = ALLOCATED;
                unhappy_list[i].last_edit_by = rank;
            } else {
                int new_proc = rand() % processes;
                unhappy_list[i].destination_proc = new_proc;
                unhappy_list[i].last_edit_by = rank;
            }
        }

    }
}

void search_first_empty(City **grid_city, int row, int col, int *x, int *y) {
    for (int i = 0; i < row; ++i) {
        for (int j = 0; j < col; ++j) {
            if (!grid_city[i][j].locked) {
                *x = i;
                *y = j;
                return;
            }
        }
        *x = -1;
        *y = -1;
    }
}


int update_with_empty_space(UnHappy *unhappy_list, City **grid_city, int rank, int unsatisfied, int processes) {
    int size = unsatisfied * processes;
    int not_allocated = 0;
    for (int i = 0; i < size; ++i) {
        if (unhappy_list[i].allocation_result == ALLOCATED && unhappy_list[i].original_proc == rank) {
            int x = 0;
            int j = 0;
            x = unhappy_list[i].x;
            j = unhappy_list[i].y;
            grid_city[x][j].status = EMPTY;
            grid_city[x][j].locked = false;
            grid_city[x][j].satisfacion = 0;
        } else if (unhappy_list[i].allocation_result == NOT_ALLOCATED) {
            ++not_allocated;
        }
    }
    printf("[RANK %d] - non alloc. %d \n", rank, not_allocated);
    return not_allocated;
}


void clear_memory(City **grid, City **cache, int row) {
    for (int i = 0; i < row; ++i) {
        free(grid[i]);
    }
    for (int i = 0; i < 2; ++i) {
        free(cache[i]);
    }


}